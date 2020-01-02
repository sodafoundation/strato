// Copyright 2019 The OpenSDS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gcp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	backendpb "github.com/opensds/multi-cloud/backend/proto"
	. "github.com/opensds/multi-cloud/s3/error"
	dscommon "github.com/opensds/multi-cloud/s3/pkg/datastore/common"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	"github.com/opensds/multi-cloud/s3/pkg/utils"
	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
	"github.com/webrtcn/s3client"
	. "github.com/webrtcn/s3client"
	"github.com/webrtcn/s3client/models"
)

type GcsAdapter struct {
	backend *backendpb.BackendDetail
	session *s3client.Client
}

func (ad *GcsAdapter) Put(ctx context.Context, stream io.Reader, object *pb.Object) (dscommon.PutResult, error) {
	bucketName := ad.backend.BucketName
	objectId := object.BucketName + "/" + object.ObjectKey
	log.Infof("put object[GCS], objectid:%s, bucket:%s\n", objectId, bucketName)

	result := dscommon.PutResult{}
	userMd5 := dscommon.GetMd5FromCtx(ctx)
	size := object.Size

	// Limit the reader to its provided size if specified.
	var limitedDataReader io.Reader
	if size > 0 { // request.ContentLength is -1 if length is unknown
		limitedDataReader = io.LimitReader(stream, size)
	} else {
		limitedDataReader = stream
	}

	bucket := ad.session.NewBucket()
	GcpObject := bucket.NewObject(bucketName)
	d, err := ioutil.ReadAll(limitedDataReader)
	data := []byte(d)
	base64Encoded, hexEncoded := utils.Md5Content(data)
	body := ioutil.NopCloser(bytes.NewReader(data))
	err = GcpObject.Create(objectId, base64Encoded, "", size, body, models.Private)
	if err != nil {
		log.Infof("put object[GCS] failed, object:%s, err:%v", objectId, err)
		return result, ErrPutToBackendFailed
	}

	calculatedMd5 := "\"" + hexEncoded + "\""
	if userMd5 != "" && userMd5 != calculatedMd5 {
		log.Error("### MD5 not match, calculatedMd5:", calculatedMd5, "userMd5:", userMd5)
		return result, ErrBadDigest
	}

	result.UpdateTime = time.Now().Unix()
	result.ObjectId = objectId
	result.Etag = calculatedMd5
	result.Written = size
	log.Infof("put object[GCS] succeed, objectId:%s, LastModified is:%v\n", objectId, result.UpdateTime)

	return result, nil
}

func (ad *GcsAdapter) Get(ctx context.Context, object *pb.Object, start int64, end int64) (io.ReadCloser, error) {
	objectId := object.ObjectId
	log.Infof("get object[GCS], objectId:%s\n", objectId)
	getObjectOption := GetObjectOption{}
	if start != 0 || end != 0 {
		rangeObj := Range{
			Begin: start,
			End:   end,
		}
		getObjectOption = GetObjectOption{
			Range: &rangeObj,
		}
	}

	bucket := ad.session.NewBucket()
	GcpObject := bucket.NewObject(ad.backend.BucketName)
	getObject, err := GcpObject.Get(objectId, &getObjectOption)
	if err != nil {
		fmt.Println(err)
		log.Infof("get object[GCS] failed, objectId:%s, err:%v", objectId, err)
		return nil, ErrGetFromBackendFailed
	}

	log.Infof("get object[GCS] succeed, objectId:%s, bytes:%d\n", objectId, getObject.ContentLength)
	return getObject.Body, nil
}

func (ad *GcsAdapter) Delete(ctx context.Context, input *pb.DeleteObjectInput) error {
	bucket := ad.session.NewBucket()
	objectId := input.Bucket + "/" + input.Key
	log.Infof("delete object[GCS], objectId:%s, err:%v\n", objectId)

	GcpObject := bucket.NewObject(ad.backend.BucketName)
	err := GcpObject.Remove(objectId)
	if err != nil {
		log.Infof("delete object[GCS] failed, objectId:%s, err:%v\n", objectId, err)
		return ErrDeleteFromBackendFailed
	}

	log.Infof("delete object[GCS] succeed, objectId:%s.\n", objectId)
	return nil
}

func (ad *GcsAdapter) ChangeStorageClass(ctx context.Context, object *pb.Object, newClass *string) error {
	log.Errorf("change storage class[gcs] is not supported.")
	return ErrInternalError
}

func (ad *GcsAdapter) InitMultipartUpload(ctx context.Context, object *pb.Object) (*pb.MultipartUpload, error) {
	bucket := ad.session.NewBucket()
	objectId := object.BucketName + "/" + object.ObjectKey
	log.Infof("init multipart upload[GCS] bucket:%s, objectId:%s\n", bucket, objectId)

	GcpObject := bucket.NewObject(ad.backend.BucketName)
	uploader := GcpObject.NewUploads(objectId)
	multipartUpload := &pb.MultipartUpload{}

	res, err := uploader.Initiate(nil)
	if err != nil {
		log.Errorf("init multipart upload[GCS] failed, objectId:%s, err:%v\n", objectId, err)
		return nil, ErrBackendInitMultipartFailed
	} else {
		log.Infof("Init multipart upload[GCS] succeed, objectId:%s, UploadId:%s\n", objectId, res.UploadID)
		multipartUpload.Bucket = object.BucketName
		multipartUpload.Key = object.ObjectKey
		multipartUpload.UploadId = res.UploadID
		multipartUpload.ObjectId = objectId
	}

	return multipartUpload, nil
}

func (ad *GcsAdapter) UploadPart(ctx context.Context, stream io.Reader, multipartUpload *pb.MultipartUpload,
	partNumber int64, upBytes int64) (*model.UploadPartResult, error) {
	objectId := multipartUpload.Bucket + "/" + multipartUpload.Key
	bucket := ad.session.NewBucket()
	log.Infof("upload part[GCS], objectId:%s, bucket:%s, partNum:%d, bytes:%s\n",
		objectId, bucket, partNumber, upBytes)

	GcpObject := bucket.NewObject(ad.backend.BucketName)
	uploader := GcpObject.NewUploads(objectId)
	d, err := ioutil.ReadAll(stream)
	data := []byte(d)
	body := ioutil.NopCloser(bytes.NewReader(data))
	contentMD5, _ := utils.Md5Content(data)
	part, err := uploader.UploadPart(int(partNumber), multipartUpload.UploadId, contentMD5, "", upBytes, body)
	if err != nil {
		log.Infof("upload part[GCS] failed, objectId:%s, partNum:%d, err:%v\n", objectId, partNumber, err)
		return nil, ErrPutToBackendFailed
	} else {
		log.Infof("upload part[CGS] objectId:%s, partNum:#%d, ETag:%s\n", objectId, partNumber, part.Etag)
		result := &model.UploadPartResult{
			Xmlns:      model.Xmlns,
			ETag:       part.Etag,
			PartNumber: partNumber}
		return result, nil
	}

	log.Error("upload part[GCS]: should not be here.")
	return nil, ErrInternalError
}

func (ad *GcsAdapter) CompleteMultipartUpload(ctx context.Context, multipartUpload *pb.MultipartUpload,
	completeUpload *model.CompleteMultipartUpload) (*model.CompleteMultipartUploadResult, error) {
	bucket := ad.session.NewBucket()
	GcpObject := bucket.NewObject(ad.backend.BucketName)
	uploader := GcpObject.NewUploads(multipartUpload.ObjectId)
	log.Infof("complete multipart upload[GCS], objectId:%s, bucket:%s\n", multipartUpload.ObjectId, bucket)

	var completeParts []CompletePart
	for _, p := range completeUpload.Parts {
		completePart := CompletePart{
			Etag:       p.ETag,
			PartNumber: int(p.PartNumber),
		}
		completeParts = append(completeParts, completePart)
	}
	resp, err := uploader.Complete(multipartUpload.UploadId, completeParts)
	if err != nil {
		log.Infof("complete multipart upload[GCS] failed, objectId:%s, err:%v\n", err)
		return nil, ErrBackendCompleteMultipartFailed
	}
	result := &model.CompleteMultipartUploadResult{
		Xmlns:    model.Xmlns,
		Location: ad.backend.Endpoint,
		Bucket:   multipartUpload.Bucket,
		Key:      multipartUpload.Key,
		ETag:     resp.Etag,
	}

	log.Infof("complete multipart upload[GCS] succeed, objectId:%s, resp:%v\n", multipartUpload.ObjectId, resp)
	return result, nil
}

func (ad *GcsAdapter) AbortMultipartUpload(ctx context.Context, multipartUpload *pb.MultipartUpload) error {
	bucket := ad.session.NewBucket()
	log.Infof("abort multipart upload[GCS], objectId:%s, bucket:%s\n", multipartUpload.ObjectId, bucket)
	GcpObject := bucket.NewObject(ad.backend.BucketName)
	uploader := GcpObject.NewUploads(multipartUpload.ObjectId)
	err := uploader.RemoveUploads(multipartUpload.UploadId)
	if err != nil {
		log.Infof("abort multipart upload[GCS] failed, objectId:%s, err:%v\n", multipartUpload.ObjectId, err)
		return ErrBackendAbortMultipartFailed
	}

	log.Infof("abort multipart upload[GCS] succeed, objectId:%s\n", multipartUpload.ObjectId)
	return nil
}

/*func (ad *GcsAdapter) ListParts(listParts *pb.ListParts, context context.Context) (*model.ListPartsOutput, S3Error) {
	newObjectKey := listParts.Bucket + "/" + listParts.Key
	bucket := ad.session.NewBucket()
	GcpObject := bucket.NewObject(ad.backend.BucketName)
	uploader := GcpObject.NewUploads(newObjectKey)

	listPartsResult, err := uploader.ListPart(listParts.UploadId)
	if err != nil {
		log.Infof("List parts failed, err:%v\n", err)
		return nil, S3Error{Code: 500, Description: err.Error()}
	} else {
		log.Infof("List parts successful\n")
		var parts []model.Part
		for _, p := range listPartsResult.Parts {
			part := model.Part{
				ETag:       p.Etag,
				PartNumber: int64(p.PartNumber),
			}
			parts = append(parts, part)
		}
		listPartsOutput := &model.ListPartsOutput{
			Xmlns:       model.Xmlns,
			Key:         listPartsResult.Key,
			Bucket:      listParts.Bucket,
			IsTruncated: listPartsResult.IsTruncated,
			MaxParts:    listPartsResult.MaxParts,
			Owner: model.Owner{
				ID:          listPartsResult.Owner.OwnerID,
				DisplayName: listPartsResult.Owner.DisplayName,
			},
			UploadId: listPartsResult.UploadID,
			Parts:    parts,
		}

		return listPartsOutput, NoError
	}
}*/

func (ad *GcsAdapter) ListParts(ctx context.Context, multipartUpload *pb.ListParts) (*model.ListPartsOutput, error) {
	return nil, ErrNotImplemented
}

func (ad *GcsAdapter) Copy(ctx context.Context, stream io.Reader, target *pb.Object) (result dscommon.PutResult, err error) {
	log.Errorf("copy[GCS] is not supported.")
	err = ErrInternalError
	return
}

func (ad *GcsAdapter) Close() error {
	//TODO
	return nil
}
