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
package alibaba

import (
	"context"
	"crypto/md5"
	_ "encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"

	backendpb "github.com/opensds/multi-cloud/backend/proto"
	. "github.com/opensds/multi-cloud/s3/error"
	dscommon "github.com/opensds/multi-cloud/s3/pkg/datastore/common"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
	"io"
)

type OSSAdapter struct {
	backend *backendpb.BackendDetail
	client  *oss.Client
}

func (ad *OSSAdapter) Put(ctx context.Context, stream io.Reader, object *pb.Object) (dscommon.PutResult, error) {

	bucket := ad.backend.BucketName
	result := dscommon.PutResult{}
	objectId := object.BucketName + "/" + object.ObjectKey
	userMd5 := dscommon.GetMd5FromCtx(ctx)
	size := object.Size

	out, err := ad.client.Bucket(bucket)
	if err != nil {
		log.Info("Access bucket failed:%v", err)
		return result, ErrInternalError
	}

	// Limit the reader to its provided size if specified.
	var limitedDataReader io.Reader
	if size > 0 { // request.ContentLength is -1 if length is unknown
		limitedDataReader = io.LimitReader(stream, size)
	} else {
		limitedDataReader = stream
	}

	md5Writer := md5.New()
	dataReader := io.TeeReader(limitedDataReader, md5Writer)
	err = out.PutObject(objectId, dataReader, oss.ObjectStorageClass(oss.StorageStandard))
	if err != nil {
		log.Info("Upload to alibaba failed:%v", err)
		return result, ErrInternalError
	} else {
		object.LastModified = time.Now().Unix()
		log.Info("LastModified is:%v\n", object.LastModified)
	}
	calculatedMd5 := hex.EncodeToString(md5Writer.Sum(nil))
	log.Debug("calculatedMd5:", calculatedMd5, ", userMd5:", userMd5)
	if userMd5 != "" && userMd5 != calculatedMd5 {
		log.Error("### MD5 not match, calculatedMd5:", calculatedMd5, "userMd5:", userMd5)
		return result, errors.New(" Error in calculating calculatedMd5")
	}

	result.UpdateTime = time.Now().Unix()
	result.ObjectId = objectId
	result.Etag = calculatedMd5
	result.Written = size
	log.Info("Upload to oss successfully.")
	return result, nil
}

func (ad *OSSAdapter) Get(ctx context.Context, object *pb.Object, start int64, end int64) (io.ReadCloser, error) {
	bucket := ad.backend.BucketName
	log.Infof("bucket is %v\n", bucket)
	log.Infof("object key %v    %v\n", object.ObjectKey, object.ObjectId)

	objectId := object.BucketName + "/" + object.ObjectKey
	alibabaBucket, err := ad.client.Bucket(bucket)
	if err != nil {
		log.Errorf("Access bucket failed:%v", err)
		return nil, ErrGetFromBackendFailed
	}
	log.Println("Start Range ", start, " End Range  ", end)
	log.Println(" objectId ", objectId, " object.ObjectKey  ", object.ObjectKey)

	body, err := alibabaBucket.GetObject(objectId, oss.Range(start, end))
	if err != nil {
		log.Errorf("download object failed:%v", err)
		return nil, ErrGetFromBackendFailed
	}

	log.Println(" download object ", body)
	return body, nil

}

func (ad *OSSAdapter) Delete(ctx context.Context, object *pb.DeleteObjectInput) error {

	bucket := ad.backend.BucketName
	objectId := object.Bucket + "/" + object.Key
	getbucket, err := ad.client.Bucket(bucket)
	if err != nil {
		log.Errorf("get bucket failed, err:%v\n", err)
		return ErrDeleteFromBackendFailed
	}
	// Delete an object.
	err = getbucket.DeleteObject(objectId)
	if err != nil {
		log.Errorf("Delete object failed, err:%v\n", err)
		return ErrDeleteFromBackendFailed
	}
	log.Infof("delete object[OSS] succeed, objectId:%s.\n", objectId)
	return nil

}

func (ad *OSSAdapter) ChangeStorageClass(ctx context.Context, object *pb.Object, newClass *string) error {

	log.Infof("change storage class[OSS] of object[%s] to %s .\n", object.ObjectId, newClass)
	bucket := ad.backend.BucketName
	objectId := object.ObjectId
	alibabaBucket, err := ad.client.Bucket(bucket)
	srcObjectKey := object.BucketName + "/" + object.ObjectKey
	var StorClass oss.StorageClassType
	switch *newClass {
	case "STANDARD_IA":
		StorClass = oss.StorageIA
	case "GLACIER":
		StorClass = oss.StorageArchive
	default:
		log.Infof("[OSS] unSpport storage class:%s", newClass)
		return ErrInvalidStorageClass
	}
	_, err = alibabaBucket.CopyObject(srcObjectKey, objectId, oss.ObjectStorageClass(StorClass))

	if err != nil {
		log.Errorf("[OSS] change storage class of object[%s] to %s failed: %v\n", object.ObjectId, newClass, err)
		return ErrPutToBackendFailed
	} else {
		log.Infof("[OSS] change storage class of object[%s] to %s succeed.\n", object.ObjectId, newClass)
	}

	return nil
}

func (ad *OSSAdapter) Copy(ctx context.Context, stream io.Reader, target *pb.Object) (result dscommon.PutResult, err error) {
	return
}

func (ad *OSSAdapter) InitMultipartUpload(ctx context.Context, object *pb.Object) (*pb.MultipartUpload, error) {

	bucket := ad.backend.BucketName
	newObjectKey := object.BucketName + "/" + object.ObjectKey
	log.Infof("bucket = %v,newObjectKey = %v\n", bucket, newObjectKey)
	multipartUpload := &pb.MultipartUpload{}
	getBucket, err := ad.client.Bucket(bucket)
	if err != nil {
		log.Infof("get bucket failed, err:%v\n", err)
		return nil, ErrInternalError

	}
	res, err := getBucket.InitiateMultipartUpload(newObjectKey)
	if err != nil {
		log.Infof("Init multipart upload failed, err:%v\n", err)
		return nil, ErrBackendInitMultipartFailed
	} else {
		log.Infof("Init s3 multipart upload succeed, UploadId:%s\n", res.UploadID)
		multipartUpload.Bucket = object.BucketName
		multipartUpload.Key = object.ObjectKey
		multipartUpload.UploadId = res.UploadID
		return multipartUpload, nil
	}
}

func (ad *OSSAdapter) UploadPart(ctx context.Context, stream io.Reader, multipartUpload *pb.MultipartUpload, partNumber int64, upBytes int64) (*model.UploadPartResult, error) {

	tries := 1
	bucket := ad.backend.BucketName
	newObjectKey := multipartUpload.Bucket + "/" + multipartUpload.Key
	input := oss.InitiateMultipartUploadResult{
		UploadID: multipartUpload.UploadId,
		Bucket:   bucket,
		Key:      newObjectKey,
	}

	getBucket, err := ad.client.Bucket(bucket)
	if err != nil {
		log.Infof("get bucket failed, err:%v\n", err)
		return nil, ErrInternalError

	}

	for tries <= 3 {
		upRes, err := getBucket.UploadPart(input, stream, upBytes, int(partNumber))
		if err != nil {
			if tries == 3 {
				log.Infof("[ERROR]Upload part to alibaba failed. err:%v\n", err)
				return nil, ErrPutToBackendFailed
			}
			log.Infof("Retrying to upload part#%d ,err:%s\n", partNumber, err)
			tries++
		} else {
			log.Infof("Uploaded part #%d, ETag:%s\n", upRes.PartNumber, upRes.ETag)
			result := &model.UploadPartResult{
				ETag:       upRes.ETag,
				PartNumber: int64(upRes.PartNumber)}

			return result, nil
		}
	}

	return nil, nil
}

func (ad *OSSAdapter) CompleteMultipartUpload(ctx context.Context, multipartUpload *pb.MultipartUpload,
	completeUpload *model.CompleteMultipartUpload) (*model.CompleteMultipartUploadResult, error) {

	bucket := ad.backend.BucketName
	newObjectKey := multipartUpload.Bucket + "/" + multipartUpload.Key
	input := oss.InitiateMultipartUploadResult{
		XMLName:  xml.Name{"", "InitiateMultipartUploadResult"},
		Bucket:   bucket,
		Key:      newObjectKey,
		UploadID: multipartUpload.UploadId,
	}

	log.Infof("etag partnumber :%v %v \n", multipartUpload.UploadId, newObjectKey)
	var completeParts []oss.UploadPart
	for _, p := range completeUpload.Parts {
		completePart := oss.UploadPart{
			ETag:       strings.ToUpper(p.ETag),
			PartNumber: int(p.PartNumber),
		}
		log.Infof("etag partnumber :%v %v \n", p.ETag, p.PartNumber)
		completeParts = append(completeParts, completePart)
	}
	//completeParts[0].ETag
	log.Infof("etag alibaba:%v \n", completeParts[0].ETag)

	getBucket, err := ad.client.Bucket(bucket)
	if err != nil {
		log.Infof("get bucket failed, err:%v\n", err)
		return nil, ErrInternalError

	}
	resp, err := getBucket.CompleteMultipartUpload(input, completeParts)
	if err != nil {
		log.Infof("completeMultipartUpload failed, err:%v\n", err)
		return nil, ErrBackendCompleteMultipartFailed
	}
	result := &model.CompleteMultipartUploadResult{
		Xmlns:    model.Xmlns,
		Location: resp.Location,
		Bucket:   multipartUpload.Bucket,
		Key:      multipartUpload.Key,
		ETag:     resp.ETag,
	}

	log.Infof("completeMultipartUpload successfully, resp:%v\n", resp)
	return result, nil
}

func (ad *OSSAdapter) AbortMultipartUpload(ctx context.Context, multipartUpload *pb.MultipartUpload) error {

	bucket := ad.backend.BucketName
	newObjectKey := multipartUpload.Bucket + "/" + multipartUpload.Key
	input := oss.InitiateMultipartUploadResult{
		UploadID: multipartUpload.UploadId,
		Bucket:   bucket,
		Key:      newObjectKey,
	}
	getBucket, err := ad.client.Bucket(bucket)
	if err != nil {
		log.Infof("get bucket failed, err:%v\n", err)
		return ErrInternalError

	}
	err = getBucket.AbortMultipartUpload(input)
	if err != nil {
		log.Infof("abortMultipartUpload failed, err:%v\n", err)
		return ErrBackendAbortMultipartFailed
	} else {
		log.Infof("abortMultipartUpload successfully.\n")
	}
	return nil
}

func (ad *OSSAdapter) ListParts(context context.Context, listParts *pb.ListParts) (*model.ListPartsOutput, error) {
	bucket := ad.backend.BucketName
	if context.Value("operation") == "listParts" {
		input := oss.InitiateMultipartUploadResult{
			UploadID: listParts.UploadId,
			Bucket:   bucket,
			Key:      listParts.Key,
		}
		getBucket, err := ad.client.Bucket(bucket)
		if err != nil {
			log.Infof("get bucket failed, err:%v\n", err)
			return nil, ErrInternalError

		}
		listPartsOutput, err := getBucket.ListUploadedParts(input)
		if err != nil {
			log.Infof("listpart failed, err:%v\n", err)
			return nil, ErrInternalError

		} else {
			listParts := &model.ListPartsOutput{}
			listParts.Bucket = listPartsOutput.Bucket
			listParts.Key = listPartsOutput.Key
			listParts.UploadId = listPartsOutput.UploadID
			listParts.MaxParts = listPartsOutput.MaxParts
			for _, p := range listPartsOutput.UploadedParts {
				parts := model.Part{
					ETag:       p.ETag,
					PartNumber: int64(p.PartNumber),
				}
				listParts.Parts = append(listParts.Parts, parts)
			}

			log.Info("ListParts successfully")
			return listParts, nil
		}
	}
	return nil, nil

}

func (ad *OSSAdapter) Close() error {
	//TODO
	return nil
}
