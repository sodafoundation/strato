// Copyright 2019 The soda Authors.
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

package ceph

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	uuid "github.com/satori/go.uuid"

	log "github.com/sirupsen/logrus"
	"github.com/webrtcn/s3client"
	. "github.com/webrtcn/s3client"
	"github.com/webrtcn/s3client/models"

	backendpb "github.com/soda/multi-cloud/backend/proto"
	. "github.com/soda/multi-cloud/s3/error"
	dscommon "github.com/soda/multi-cloud/s3/pkg/datastore/common"
	"github.com/soda/multi-cloud/s3/pkg/model"
	"github.com/soda/multi-cloud/s3/pkg/utils"
	pb "github.com/soda/multi-cloud/s3/proto"
)

const sampleBucket string = "sample"

type CephAdapter struct {
	backend *backendpb.BackendDetail
	session *s3client.Client
}

func (ad *CephAdapter) BucketDelete(ctx context.Context, in *pb.Bucket) error {
	bucket := ad.session.NewBucket()
	err := bucket.Remove(in.Name)
	if err != nil {
		log.Error("the delete bucket failed in ceph service with err: %s", err.Error())
		return err
	}
	log.Debug("The bucket:%s deleted successfully in ceph", in.Name)
	return nil
}

func (ad *CephAdapter) BucketCreate(ctx context.Context, input *pb.Bucket) error {
	bucket := ad.session.NewBucket()
	err := bucket.Create(input.Name, models.Private)
	if err != nil {
		log.Error("the create bucket failed in ceph service with err:%s", err.Error())
		return err
	}
	log.Debug("The bucket:%s created successfully in ceph", input.Name)
	return nil
}

func (ad *CephAdapter) Put(ctx context.Context, stream io.Reader, object *pb.Object) (result dscommon.PutResult, err error) {
	bucketName := object.BucketName
	objectId := object.ObjectKey
	log.Infof("put object[Ceph S3], bucket:%s, objectId:%s\n", bucketName, objectId)

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
	cephObject := bucket.NewObject(bucketName)
	d, err := ioutil.ReadAll(limitedDataReader)
	data := []byte(d)
	length := int64(len(d))
	base64Encoded, hexEncoded := utils.Md5Content(data)
	body := ioutil.NopCloser(bytes.NewReader(data))
	err = cephObject.Create(objectId, base64Encoded, "", length, body, models.Private)
	log.Infof("put object[Ceph S3] end, objectId:%s\n", objectId)
	if err != nil {
		log.Infof("upload object[Ceph S3] failed, objectId:%s, err:%v", objectId, err)
		return result, ErrPutToBackendFailed
	}

	calculatedMd5 := "\"" + hexEncoded + "\""
	if userMd5 != "" && userMd5 != calculatedMd5 {
		log.Error("### MD5 not match, calculatedMd5:", calculatedMd5, "userMd5:", userMd5)
		return result, ErrBadDigest
	}

	result.UpdateTime = time.Now().Unix()
	result.ObjectId = objectId
	result.Etag = hexEncoded
	result.Written = length
	log.Infof("upload object[Ceph S3] succeed, objectId:%s, UpdateTime is:%v, etag:\n", objectId,
		result.UpdateTime, result.Etag)

	return result, nil
}

func (ad *CephAdapter) Get(ctx context.Context, object *pb.Object, start int64, end int64) (io.ReadCloser, error) {
	log.Infof("get object[Ceph S3], bucket:%s, objectId:%s\n", object.BucketName, object.ObjectId)

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
	cephObject := bucket.NewObject(object.BucketName)
	getObject, err := cephObject.Get(object.ObjectId, &getObjectOption)
	if err != nil {
		fmt.Println(err)
		log.Infof("get object[Ceph S3], objectId:%s failed:%v", object.ObjectId, err)
		return nil, ErrGetFromBackendFailed
	}

	log.Infof("get object[Ceph S3] succeed, objectId:%s, bytes:%d\n", object.ObjectId, getObject.ContentLength)
	return getObject.Body, nil
}

func (ad *CephAdapter) Delete(ctx context.Context, object *pb.DeleteObjectInput) error {
	bucket := ad.session.NewBucket()
	objectId := object.Key
	log.Infof("delete object[Ceph S3], objectId:%s, bucket:%s\n", objectId, bucket)

	cephObject := bucket.NewObject(object.Bucket)
	err := cephObject.Remove(objectId)
	if err != nil {
		log.Infof("delete object[Ceph S3] failed, objectId:%s, err:%v\n", objectId, err)
		return ErrDeleteFromBackendFailed
	}

	log.Infof("delete object[Ceph S3] succeed, objectId:%s.\n", objectId)
	return nil
}

func (ad *CephAdapter) Copy(ctx context.Context, stream io.Reader, target *pb.Object) (result dscommon.PutResult, err error) {
	log.Errorf("copy[Ceph S3] is not supported.")
	err = ErrInternalError
	return
}

func (ad *CephAdapter) ChangeStorageClass(ctx context.Context, object *pb.Object, newClass *string) error {
	log.Errorf("change storage class[Ceph S3] is not supported.")
	return ErrInternalError
}

/*func (ad *CephAdapter) GetObjectInfo(context context.Context, bucketName string, key string) (*pb.Object, error) {
	bucket := ad.backend.BucketName
	newKey := bucketName + "/" + key

	bucketO := ad.session.NewBucket()
	bucketResp, err := bucketO.Get(bucket, newKey, "", "", 1000)
	if err != nil {
		log.Infof("error occured during get Object Info, err:%v\n", err)
		return nil, err
	}

	for _, content := range bucketResp.Contents {
		realKey := bucketName + "/" + key
		if realKey != content.Key {
			break
		}
		obj := &pb.Object{
			BucketName: bucketName,
			ObjectKey:  key,
			Size:       content.Size,
		}

		return obj, nil
	}

	log.Infof("can not find specified object(%s).\n", key)
	return nil, NoSuchObject.Error()
}*/

func (ad *CephAdapter) InitMultipartUpload(ctx context.Context, object *pb.Object) (*pb.MultipartUpload, error) {
	bucket := ad.session.NewBucket()
	objectId := object.ObjectKey
	log.Infof("init multipart upload[Ceph S3], bucket = %s,objectId = %v\n", bucket, objectId)
	cephObject := bucket.NewObject(object.BucketName)
	uploader := cephObject.NewUploads(objectId)
	multipartUpload := &pb.MultipartUpload{}

	res, err := uploader.Initiate(nil)
	if err != nil {
		log.Errorf("init multipart upload[Ceph S3] failed, objectId:%s, err:%v\n", objectId, err)
		return nil, err
	} else {
		log.Infof("init multipart upload[Ceph S3] succeed, objectId:%s, UploadId:%s\n", objectId, res.UploadID)
		multipartUpload.Bucket = object.BucketName
		multipartUpload.Key = object.ObjectKey
		multipartUpload.UploadId = res.UploadID
		multipartUpload.ObjectId = objectId
		return multipartUpload, nil
	}
}

func (ad *CephAdapter) UploadPart(ctx context.Context, stream io.Reader, multipartUpload *pb.MultipartUpload,
	partNumber int64, upBytes int64) (*model.UploadPartResult, error) {
	bucket := ad.session.NewBucket()
	log.Infof("upload part[Ceph S3], objectId:%s, bucket:%s\n", multipartUpload.ObjectId, bucket)

	cephObject := bucket.NewObject(multipartUpload.Bucket)
	uploader := cephObject.NewUploads(multipartUpload.ObjectId)

	d, err := ioutil.ReadAll(stream)
	data := []byte(d)
	body := ioutil.NopCloser(bytes.NewReader(data))
	contentMD5, _ := utils.Md5Content(data)
	part, err := uploader.UploadPart(int(partNumber), multipartUpload.UploadId, contentMD5, "", upBytes, body)
	if err != nil {
		log.Errorf("upload part[Ceph S3] failed, err:%v\n", err)
		return nil, ErrPutToBackendFailed
	} else {
		log.Infof("uploaded part[Ceph S3] #%d successfully, ETag:%s\n", partNumber, part.Etag)
		result := &model.UploadPartResult{
			Xmlns:      model.Xmlns,
			ETag:       part.Etag,
			PartNumber: partNumber}
		return result, nil
	}

	log.Error("upload part[Ceph S3]: should not be here.")
	return nil, ErrInternalError
}

func (ad *CephAdapter) CompleteMultipartUpload(ctx context.Context, multipartUpload *pb.MultipartUpload,
	completeUpload *model.CompleteMultipartUpload) (*model.CompleteMultipartUploadResult, error) {
	bucket := ad.session.NewBucket()
	log.Infof("complete multipart upload[Ceph S3], objectId:%s, bucket:%s\n", multipartUpload.ObjectId, bucket)

	cephObject := bucket.NewObject(multipartUpload.Bucket)
	uploader := cephObject.NewUploads(multipartUpload.ObjectId)
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
		log.Infof("complete multipart upload[Ceph S3] failed, objectId:%s, err:%v\n", multipartUpload.ObjectId, err)
		return nil, ErrBackendCompleteMultipartFailed
	}
	result := &model.CompleteMultipartUploadResult{
		Xmlns:    model.Xmlns,
		Location: ad.backend.Endpoint,
		Bucket:   multipartUpload.Bucket,
		Key:      multipartUpload.Key,
		ETag:     resp.Etag,
	}

	log.Infof("complete multipart upload[Ceph S3] succeed, objectId:%s, resp:%v\n", multipartUpload.ObjectId, resp)
	return result, nil
}

func (ad *CephAdapter) AbortMultipartUpload(ctx context.Context, multipartUpload *pb.MultipartUpload) error {
	bucket := ad.session.NewBucket()
	cephObject := bucket.NewObject(multipartUpload.Bucket)
	uploader := cephObject.NewUploads(multipartUpload.ObjectId)
	log.Infof("abort multipart upload[Ceph S3], objectId:%s, bucket:%s\n", multipartUpload.ObjectId, bucket)

	err := uploader.RemoveUploads(multipartUpload.UploadId)
	if err != nil {
		log.Infof("abort multipart upload[Ceph S3] failed, objectId:%s, err:%v\n", multipartUpload.ObjectId, err)
		return ErrBackendAbortMultipartFailed
	} else {
		log.Infof("abort multipart upload[Ceph S3] succeed, objectId:%s, err:%v\n", multipartUpload.ObjectId, err)
	}

	return nil
}

/*func (ad *CephAdapter) ListParts(context context.Context, listParts *pb.ListParts) (*model.ListPartsOutput, error) {
	newObjectKey := listParts.Bucket + "/" + listParts.Key
	bucket := ad.session.NewBucket()
	cephObject := bucket.NewObject(ad.backend.BucketName)
	uploader := cephObject.NewUploads(newObjectKey)

	listPartsResult, err := uploader.ListPart(listParts.UploadId)
	if err != nil {
		log.Infof("list parts failed, err:%v\n", err)
		return nil, S3Error{Code: 500, Description: err.Error()}.Error()
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

		return listPartsOutput, nil
	}
}*/

func (ad *CephAdapter) ListParts(ctx context.Context, multipartUpload *pb.ListParts) (*model.ListPartsOutput, error) {
	return nil, ErrNotImplemented
}

func (ad *CephAdapter) BackendCheck(ctx context.Context, backendDetail *pb.BackendDetailS3) error {
	randId := uuid.NewV4().String()
	input := &pb.Bucket{
		Name: sampleBucket + randId,
	}
	for i := 1; i <= 3; i++ {
		err := ad.BucketCreate(ctx, input)
		if err == nil {
			log.Debug("Create sample bucket is successul")
			break
		}
		if i == 3 {
			log.Error("failed to create sample bucket :", err)
			return err
		}
	}

	for j := 1; j <= 10; j++ {
		bucket := ad.session.NewBucket()
		_, err1 := bucket.Get(input.Name, "", "", "", 1000)
		if err1 == nil {
			break
		}
		if j == 10 {
			log.Error("failed to get bucket Info :", err1)
			return err1
		}
		time.Sleep(5 * time.Second)
	}
	err := ad.BucketDelete(ctx, input)
	if err != nil {
		log.Error("failed to delete sample bucket :", err)
		return err
	}

	log.Debug("Delete sample bucket is successful")
	return nil
}

func (ad *CephAdapter) Restore(ctx context.Context, inp *pb.Restore) error {
	return ErrNotImplemented
}

func (ad *CephAdapter) Close() error {
	// TODO
	return nil
}
