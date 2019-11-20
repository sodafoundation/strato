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

package hws

import (
	"context"
	"io"
	"time"

	"github.com/opensds/multi-cloud/api/pkg/utils/obs"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	. "github.com/opensds/multi-cloud/s3/error"
	dscommon "github.com/opensds/multi-cloud/s3/pkg/datastore/common"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	osdss3 "github.com/opensds/multi-cloud/s3/pkg/service"
	"github.com/opensds/multi-cloud/s3/pkg/utils"
	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

type OBSAdapter struct {
	backend *backendpb.BackendDetail
	client  *obs.ObsClient
}

/*func Init(backend *backendpb.BackendDetail) *OBSAdapter {
	endpoint := backend.Endpoint
	AccessKeyID := backend.Access
	AccessKeySecret := backend.Security

	client, err := obs.New(AccessKeyID, AccessKeySecret, endpoint)

	if err != nil {
		log.Infof("Access obs failed:%v", err)
		return nil
	}

	adap := &OBSAdapter{backend: backend, client: client}
	return adap
}*/

func (ad *OBSAdapter) Put(ctx context.Context, stream io.Reader, object *pb.Object) (dscommon.PutResult, error) {
	bucket := ad.backend.BucketName
	objectId := object.BucketName + "/" + object.ObjectKey
	log.Infof("put object[OBS], objectId:%s, bucket:%s\n", objectId, bucket)

	result := dscommon.PutResult{}

	if object.Tier == 0 {
		// default
		object.Tier = utils.Tier1
	}
	storClass, err := osdss3.GetNameFromTier(object.Tier, utils.OSTYPE_OBS)
	if err != nil {
		log.Infof("translate tier[%d] to aws storage class failed\n", object.Tier)
		return result, ErrInternalError
	}

	input := &obs.PutObjectInput{}
	input.Bucket = bucket
	input.Key = objectId
	input.Body = stream
	input.StorageClass = obs.StorageClassType(storClass)

	log.Infof("upload object[OBS] begin, objectId:%s\n", objectId)
	out, err := ad.client.PutObject(input)
	log.Infof("upload object[OBS] end, objectId:%s\n", objectId)
	if err != nil {
		log.Infof("upload object[OBS] failed, objectId:%s, err:%v", objectId, err)
		return result, ErrPutToBackendFailed
	}

	result.ObjectId = objectId
	result.UpdateTime = time.Now().Unix()
	log.Infof("upload object[OBS] succeed, objectId:%s, UpdateTime is:%v\n", objectId, result.UpdateTime)
	result.Etag = out.ETag

	return result, nil
}

func (ad *OBSAdapter) Get(ctx context.Context, object *pb.Object, start int64, end int64) (io.ReadCloser, error) {
	bucket := ad.backend.BucketName
	objectId := object.ObjectId
	log.Infof("get object[OBS], objectId:%s, bucket:%s\n", objectId, bucket)

	input := &obs.GetObjectInput{}
	input.Bucket = bucket
	input.Key = objectId
	if start != 0 || end != 0 {
		input.RangeStart = start
		input.RangeEnd = end
	}

	out, err := ad.client.GetObject(input)
	if err != nil {
		log.Infof("get object[OBS] failed, objectId:%,s err:%v", objectId, err)
		return nil, ErrGetFromBackendFailed
	}

	log.Infof("get object[OBS] suceed, objectId:%s\n", objectId)
	return out.Body, nil
}

func (ad *OBSAdapter) Delete(ctx context.Context, object *pb.DeleteObjectInput) error {
	objectId := object.Bucket + "/" + object.Key
	log.Infof("delete object[OBS], objectId:%s\n", objectId)

	deleteObjectInput := obs.DeleteObjectInput{Bucket: ad.backend.BucketName, Key: objectId}
	_, err := ad.client.DeleteObject(&deleteObjectInput)
	if err != nil {
		log.Infof("delete object[OBS] failed, objectId:%s, :%v", objectId, err)
		return ErrDeleteFromBackendFailed
	}

	log.Infof("delete object[OBS] succeed, objectId:%s.\n", objectId)
	return nil
}

func (ad *OBSAdapter) ChangeStorageClass(ctx context.Context, object *pb.Object, newClass *string) error {
	log.Infof("change storage class[OBS] of object[%s] to %s .\n", object.ObjectId, newClass)

	input := &obs.CopyObjectInput{}
	input.Bucket = ad.backend.BucketName
	input.Key = object.ObjectId
	input.CopySourceBucket = ad.backend.BucketName
	input.CopySourceKey = object.ObjectId
	input.MetadataDirective = obs.CopyMetadata
	switch *newClass {
	case "STANDARD_IA":
		input.StorageClass = obs.StorageClassWarm
	case "GLACIER":
		input.StorageClass = obs.StorageClassCold
	default:
		log.Infof("[OBS] unspport storage class:%s", newClass)
		return ErrInvalidStorageClass
	}
	_, err := ad.client.CopyObject(input)
	if err != nil {
		log.Errorf("[OBS] change storage class of object[%s] to %s failed: %v\n", object.ObjectId, newClass, err)
		return ErrPutToBackendFailed
	} else {
		log.Infof("[OBS] change storage class of object[%s] to %s succeed.\n", object.ObjectId, newClass)
	}

	return nil
}

func (ad *OBSAdapter) Copy(ctx context.Context, stream io.Reader, target *pb.Object) (result dscommon.PutResult, err error) {
	return
}

/*func (ad *OBSAdapter) GetObjectInfo(bucketName string, key string, context context.Context) (*pb.Object, S3Error) {
	return nil, nil
}*/

func (ad *OBSAdapter) InitMultipartUpload(ctx context.Context, object *pb.Object) (*pb.MultipartUpload, error) {
	bucket := ad.backend.BucketName
	objectId := object.BucketName + "/" + object.ObjectKey
	multipartUpload := &pb.MultipartUpload{}
	log.Infof("init multipart upload[OBS], objectId:%s, bucket:%s\n", objectId, bucket)

	input := &obs.InitiateMultipartUploadInput{}
	input.Bucket = bucket
	input.Key = objectId
	input.StorageClass = obs.StorageClassStandard // Currently, only support standard.
	out, err := ad.client.InitiateMultipartUpload(input)
	if err != nil {
		log.Infof("init multipart upload[OBS] failed, objectId:%s, err:%v", objectId, err)
		return nil, ErrBackendInitMultipartFailed
	}

	multipartUpload.Bucket = out.Bucket
	multipartUpload.Key = out.Key
	multipartUpload.UploadId = out.UploadId
	log.Infof("init multipart upload[OBS] succeed, objectId:%s\n", objectId)
	return multipartUpload, nil
}

func (ad *OBSAdapter) UploadPart(ctx context.Context, stream io.Reader, multipartUpload *pb.MultipartUpload,
	partNumber int64, upBytes int64) (*model.UploadPartResult, error) {
	bucket := ad.backend.BucketName
	objectId := multipartUpload.Bucket + "/" + multipartUpload.Key
	log.Infof("upload part[OBS], objectId:%s, partNum:%d, bytes:%d\n", objectId, partNumber, upBytes)

	input := &obs.UploadPartInput{}
	input.Bucket = bucket
	input.Key = objectId
	input.Body = stream
	input.PartNumber = int(partNumber)
	input.PartSize = upBytes
	log.Infof(" multipartUpload.UploadId is %v", multipartUpload.UploadId)
	input.UploadId = multipartUpload.UploadId
	out, err := ad.client.UploadPart(input)

	if err != nil {
		log.Infof("upload part[OBS] failed, objectId:%s, err:%v", objectId, err)
		return nil, ErrPutToBackendFailed
	}

	log.Infof("upload part[OBS] succeed, objectId:%s, partNum:%d\n", objectId, out.PartNumber)
	result := &model.UploadPartResult{ETag: out.ETag, PartNumber: partNumber}

	return result, nil
}

func (ad *OBSAdapter) CompleteMultipartUpload(ctx context.Context, multipartUpload *pb.MultipartUpload,
	completeUpload *model.CompleteMultipartUpload) (*model.CompleteMultipartUploadResult, error) {
	bucket := ad.backend.BucketName
	objectId := multipartUpload.Bucket + "/" + multipartUpload.Key
	log.Infof("complete multipart upload[OBS], objectId:%s, bucket:%s\n", objectId, bucket)

	input := &obs.CompleteMultipartUploadInput{}
	input.Bucket = bucket
	input.Key = objectId
	input.UploadId = multipartUpload.UploadId
	for _, p := range completeUpload.Parts {
		part := obs.Part{
			PartNumber: int(p.PartNumber),
			ETag:       p.ETag,
		}
		input.Parts = append(input.Parts, part)
	}
	resp, err := ad.client.CompleteMultipartUpload(input)
	result := &model.CompleteMultipartUploadResult{
		Xmlns:    model.Xmlns,
		Location: resp.Location,
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		ETag:     resp.ETag,
	}
	if err != nil {
		log.Infof("complete multipart upload[OBS] failed, objectid:%s, err:%v", objectId, err)
		return nil, ErrBackendCompleteMultipartFailed
	}

	log.Infof("complete multipart upload[OBS] succeed, objectId:%s\n", objectId)
	return result, nil
}

func (ad *OBSAdapter) AbortMultipartUpload(ctx context.Context, multipartUpload *pb.MultipartUpload) error {
	bucket := ad.backend.BucketName
	objectId := multipartUpload.Bucket + "/" + multipartUpload.Key
	log.Infof("abort multipart upload[OBS], objectId:%s, bucket:%s\n", objectId, bucket)

	input := &obs.AbortMultipartUploadInput{}
	input.UploadId = multipartUpload.UploadId
	input.Bucket = bucket
	input.Key = objectId
	_, err := ad.client.AbortMultipartUpload(input)
	if err != nil {
		log.Infof("abort multipart upload[OBS] failed, objectId:%s, err:%v", objectId, err)
		return ErrBackendAbortMultipartFailed
	}

	log.Infof("abort multipart upload[OBS] succeed, objectId:%s\n", objectId)
	return nil
}

/*func (ad *OBSAdapter) ListParts(listParts *pb.ListParts, context context.Context) (*model.ListPartsOutput, S3Error) {
	bucket := ad.backend.BucketName
	if context.Value("operation") == "listParts" {
		input := &obs.ListPartsInput{}
		input.Bucket = bucket
		input.Key = listParts.Key
		input.UploadId = listParts.UploadId
		input.MaxParts = int(listParts.MaxParts)
		listPartsOutput, err := ad.client.ListParts(input)
		listParts := &model.ListPartsOutput{}
		listParts.Bucket = listPartsOutput.Bucket
		listParts.Key = listPartsOutput.Key
		listParts.UploadId = listPartsOutput.UploadId
		listParts.MaxParts = listPartsOutput.MaxParts

		for _, p := range listPartsOutput.Parts {
			part := model.Part{
				PartNumber: int64(p.PartNumber),
				ETag:       p.ETag,
			}
			listParts.Parts = append(listParts.Parts, part)
		}

		if err != nil {
			log.Infof("ListPartsListParts is nil:%v\n", err)
			return nil, S3Error{Code: 500, Description: "AbortMultipartUploadInput failed"}
		} else {
			log.Infof("ListParts successfully")
			return listParts, NoError
		}
	}
	return nil, NoError
}*/

func (ad *OBSAdapter) Close() error {
	//TODO
	return nil
}
