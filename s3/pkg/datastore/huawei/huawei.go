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

	"encoding/base64"
	"encoding/hex"
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

func (ad *OBSAdapter) Put(ctx context.Context, stream io.Reader, object *pb.Object) (dscommon.PutResult, error) {
	bucket := ad.backend.BucketName
	objectId := object.BucketName + "/" + object.ObjectKey
	result := dscommon.PutResult{}
	userMd5 := dscommon.GetMd5FromCtx(ctx)
	size := object.Size
	log.Infof("put object[OBS], objectId:%s, bucket:%s, size=%d, userMd5=%s\n", objectId, bucket, size, userMd5)

	if object.Tier == 0 {
		// default
		object.Tier = utils.Tier1
	}
	storClass, err := osdss3.GetNameFromTier(object.Tier, utils.OSTYPE_OBS)
	if err != nil {
		log.Errorf("translate tier[%d] to aws storage class failed\n", object.Tier)
		return result, ErrInternalError
	}

	input := &obs.PutObjectInput{}
	input.Bucket = bucket
	input.Key = objectId
	input.Body = stream
	input.ContentLength = size
	input.StorageClass = obs.StorageClassType(storClass)
	if userMd5 != "" {
		md5Bytes, err := hex.DecodeString(userMd5)
		if err != nil {
			log.Warnf("user input md5 is abandoned, cause decode md5 failed, err:%v\n", err)
		} else {
			input.ContentMD5 = base64.StdEncoding.EncodeToString(md5Bytes)
			log.Debugf("input.ContentMD5=%s\n", input.ContentMD5)
		}
	}

	log.Infof("upload object[OBS] begin, objectId:%s\n", objectId)
	out, err := ad.client.PutObject(input)
	log.Infof("upload object[OBS] end, objectId:%s\n", objectId)
	if err != nil {
		log.Errorf("upload object[OBS] failed, objectId:%s, err:%v", objectId, err)
		return result, ErrPutToBackendFailed
	}

	result.Etag = dscommon.TrimQuot(out.ETag)
	if userMd5 != "" && userMd5 != result.Etag {
		log.Error("### MD5 not match, result.Etag:", result.Etag, ", userMd5:", userMd5)
		return result, ErrBadDigest
	}

	result.ObjectId = objectId
	result.UpdateTime = time.Now().Unix()
	result.Meta = out.VersionId
	result.Written = size
	log.Infof("upload object[OBS] succeed, objectId:%s, UpdateTime is:%v\n", objectId, result.UpdateTime)

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

	log.Infof("get object[OBS] succeed, objectId:%s\n", objectId)
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
	storClass, err := osdss3.GetNameFromTier(object.Tier, utils.OSTYPE_OBS)
	if err != nil {
		log.Errorf("translate tier[%d] to obs storage class failed\n", object.Tier)
		return nil, ErrInternalError
	}
	input.StorageClass = obs.StorageClassType(storClass)

	out, err := ad.client.InitiateMultipartUpload(input)
	if err != nil {
		log.Infof("init multipart upload[OBS] failed, objectId:%s, err:%v", objectId, err)
		return nil, ErrBackendInitMultipartFailed
	}

	multipartUpload.Bucket = out.Bucket
	multipartUpload.Key = out.Key
	multipartUpload.UploadId = out.UploadId
	multipartUpload.ObjectId = objectId
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
	if err != nil {
		log.Errorf("complete multipart upload[OBS] failed, objectid:%s, err:%v\n", objectId, err)
		return nil, ErrBackendCompleteMultipartFailed
	}
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

func (ad *OBSAdapter) ListParts(context context.Context, listParts *pb.ListParts) (*model.ListPartsOutput, error) {
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
			return nil, err
		} else {
			log.Infof("ListParts successfully")
			return listParts, nil
		}
	}
	return nil, nil
}

func (ad *OBSAdapter) Close() error {
	//TODO
	return nil
}
