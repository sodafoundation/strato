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

package service

import (
	"context"
	"io"
	"time"

	"github.com/opensds/multi-cloud/api/pkg/s3/datatype"
	. "github.com/opensds/multi-cloud/s3/error"
	dscommon "github.com/opensds/multi-cloud/s3/pkg/datastore/common"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/driver"
	. "github.com/opensds/multi-cloud/s3/pkg/meta/types"
	"github.com/opensds/multi-cloud/s3/pkg/meta/util"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	"github.com/opensds/multi-cloud/s3/pkg/utils"
	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

const (
	MAX_PART_SIZE   = 5 << 30 // 5GB, max object size in single upload
	MAX_PART_NUMBER = 10000   // max upload part number in one multipart upload
)

func (s *s3Service) ListBucketUploadRecords(ctx context.Context, in *pb.ListBucketUploadRequest, out *pb.ListBucketUploadResponse) error {
	log.Info("ListBucketUploadRecords is called in s3 service.")
	bucketName := in.BucketName

	var err error
	defer func() {
		out.ErrorCode = GetErrCode(err)
	}()

	bucket, err := s.MetaStorage.GetBucket(ctx, bucketName, true)
	if err != nil {
		log.Errorln("failed to get bucket meta. err:", err)
		return err
	}

	isAdmin, tenantId, _, err := util.GetCredentialFromCtx(ctx)
	if err != nil && isAdmin == false {
		log.Error("get tenant id failed")
		err = ErrInternalError
		return err
	}

	if !isAdmin {
		switch bucket.Acl.CannedAcl {
		case "public-read", "public-read-write":
			break
		default:
			if bucket.TenantId != tenantId {
				log.Errorln("bucket owner is not equal to request owner.")
				err = ErrBucketAccessForbidden
				return err
			}
		}
	}

	result, err := s.MetaStorage.Db.ListMultipartUploads(in)
	if err != nil {
		log.Errorln("failed to list multipart uploads in meta storage. err:", err)
		return err
	}
	out.Result = result

	log.Infoln("List bucket multipart uploads successfully.")
	return nil
}

func (s *s3Service) InitMultipartUpload(ctx context.Context, in *pb.InitMultiPartRequest, out *pb.InitMultiPartResponse) error {
	log.Info("InitMultipartUpload is called in s3 service.")
	bucketName := in.BucketName
	objectKey := in.ObjectKey

	var err error
	defer func() {
		out.ErrorCode = GetErrCode(err)
	}()

	isAdmin, tenantId, _, err := util.GetCredentialFromCtx(ctx)
	if err != nil && isAdmin == false {
		log.Error("get tenant id failed")
		err = ErrInternalError
		return err
	}

	bucket, err := s.MetaStorage.GetBucket(ctx, bucketName, true)
	if err != nil {
		log.Errorln("failed to get bucket from meta storage. err:", err)
		return err
	}

	if !isAdmin {
		switch bucket.Acl.CannedAcl {
		case "public-read-write":
			break
		default:
			if bucket.TenantId != tenantId {
				log.Errorln("bucket owner is not equal to request owner.")
				err = ErrBucketAccessForbidden
				return err
			}
		}
	}

	attrs := in.Attrs
	contentType, ok := attrs["Content-Type"]
	if !ok {
		contentType = "application/octet-stream"
	}

	backendName := bucket.DefaultLocation
	backend, err := utils.GetBackend(ctx, s.backendClient, backendName)
	if err != nil {
		log.Errorln("failed to get backend client with err:", err)
		return err
	}

	sd, err := driver.CreateStorageDriver(backend.Type, backend)
	if err != nil {
		log.Errorln("failed to create storage. err:", err)
		return err
	}
	res, err := sd.InitMultipartUpload(ctx, &pb.Object{BucketName: bucketName, ObjectKey: objectKey})
	if err != nil {
		log.Errorln("failed to init multipart upload. err:", err)
		return err
	}

	multipartMetadata := MultipartMetadata{
		InitiatorId:  tenantId,
		TenantId:     bucket.TenantId,
		UserId:       bucket.UserId,
		ContentType:  contentType,
		Acl:          datatype.Acl{CannedAcl: in.Acl.CannedAcl},
		Attrs:        attrs,
		StorageClass: StorageClass(in.StorageClass),
	}

	multipart := Multipart{
		BucketName:  bucketName,
		ObjectKey:   objectKey,
		UploadId:    res.UploadId,
		ObjectId:    res.ObjectId,
		InitialTime: time.Now().UTC(),
		Metadata:    multipartMetadata,
	}

	err = s.MetaStorage.Db.CreateMultipart(multipart)
	if err != nil {
		log.Errorln("failed to create multipart in meta. err:", err)
		return err
	}
	out.UploadID = res.UploadId

	return nil
}

func (s *s3Service) UploadPart(ctx context.Context, stream pb.S3_UploadPartStream) error {
	log.Info("UploadPart is called in s3 service.")
	var err error
	uploadResponse := pb.UploadPartResponse{}
	defer func() {
		uploadResponse.ErrorCode = GetErrCode(err)
		stream.SendMsg(&uploadResponse)
	}()

	uploadRequest := pb.UploadPartRequest{}
	err = stream.RecvMsg(&uploadRequest)
	if err != nil {
		log.Errorln("failed to receive msg. err:", err)
		return err
	}
	bucketName := uploadRequest.BucketName
	objectKey := uploadRequest.ObjectKey
	partId := uploadRequest.PartId
	uploadId := uploadRequest.UploadId
	size := uploadRequest.Size

	log.Infof("receive msg, bucketname:%v, objectkey:%v, partId:%v, uploadId:%v,size:%v", bucketName, objectKey, partId, uploadId, size)
	multipart, err := s.MetaStorage.GetMultipart(bucketName, objectKey, uploadId)
	if err != nil {
		log.Infoln("failed to get multipart. err:", err)
		return err
	}

	if size > MAX_PART_SIZE {
		log.Errorf("object part size is too large. size:", size)
		err = ErrEntityTooLarge
		return err
	}

	isAdmin, tenantId, _, err := util.GetCredentialFromCtx(ctx)
	if err != nil && isAdmin == false {
		log.Error("get tenant id failed")
		err = ErrInternalError
		return err
	}

	bucket, err := s.MetaStorage.GetBucket(ctx, bucketName, true)
	if err != nil {
		log.Errorln("get bucket failed with err:", err)
		return err
	}

	if !isAdmin {
		switch bucket.Acl.CannedAcl {
		case "public-read-write":
			break
		default:
			if bucket.TenantId != tenantId {
				err = ErrBucketAccessForbidden
				log.Errorln("bucket owner is not equal to request owner.")
				return err
			}
		}
	}

	backendName := bucket.DefaultLocation
	backend, err := utils.GetBackend(ctx, s.backendClient, backendName)
	if err != nil {
		log.Errorln("failed to get backend client with err:", err)
		return err
	}

	data := &StreamReader{in: stream}
	limitedDataReader := io.LimitReader(data, size)
	sd, err := driver.CreateStorageDriver(backend.Type, backend)
	if err != nil {
		log.Errorln("failed to create storage. err:", err)
		return nil
	}
	ctx = context.WithValue(ctx, dscommon.CONTEXT_KEY_MD5, uploadRequest.Md5Hex)
	log.Infoln("bucketname:", bucketName, " objectKey:", objectKey, " uploadid:", uploadId, " objectId:", multipart.ObjectId, " partid:", partId)
	result, err := sd.UploadPart(ctx, limitedDataReader, &pb.MultipartUpload{
		Bucket:   bucketName,
		Key:      objectKey,
		UploadId: uploadId,
		ObjectId: multipart.ObjectId},
		int64(partId), size)
	if err != nil {
		log.Errorln("failed to upload part to backend. err:", err)
		return err
	}
	uploadResponse.ETag = result.ETag

	log.Infoln("UploadPart upload part successfully.")
	return nil
}

func (s *s3Service) CompleteMultipartUpload(ctx context.Context, in *pb.CompleteMultipartRequest, out *pb.CompleteMultipartResponse) error {
	log.Info("CompleteMultipartUpload is called in s3 service.")
	bucketName := in.BucketName
	objectKey := in.ObjectKey
	uploadId := in.UploadId

	var err error
	defer func() {
		out.ErrorCode = GetErrCode(err)
	}()

	isAdmin, tenantId, _, err := util.GetCredentialFromCtx(ctx)
	if err != nil && isAdmin == false {
		log.Error("get tenant id failed")
		err = ErrInternalError
		return err
	}

	bucket, err := s.MetaStorage.GetBucket(ctx, bucketName, true)
	if err != nil {
		log.Errorf("failed to get bucket from meta stoarge. err:", err)
		return err
	}

	if !isAdmin {
		switch bucket.Acl.CannedAcl {
		case "public-read-write":
			break
		default:
			if bucket.TenantId != tenantId {
				log.Errorln("bucket owner is not equal to request owner.")
				err = ErrBucketAccessForbidden
				return err
			}
		}
	}

	multipart, err := s.MetaStorage.GetMultipart(bucketName, objectKey, uploadId)
	if err != nil {
		log.Errorln("failed to get multipart info. err:", err)
		return err
	}

	backendName := bucket.DefaultLocation
	backend, err := utils.GetBackend(ctx, s.backendClient, backendName)
	if err != nil {
		log.Errorln("failed to get backend client with err:", err)
		return err
	}
	sd, err := driver.CreateStorageDriver(backend.Type, backend)
	if err != nil {
		log.Errorln("failed to create storage. err:", err)
		return nil
	}

	parts := make([]model.Part, 0)
	completeUpload := &model.CompleteMultipartUpload{}
	for _, part := range in.CompleteParts {
		parts = append(parts, model.Part{
			PartNumber: part.PartNumber,
			ETag:       part.ETag,
		})
	}
	completeUpload.Parts = parts
	result, err := sd.CompleteMultipartUpload(ctx, &pb.MultipartUpload{
		Bucket:   bucketName,
		Key:      objectKey,
		UploadId: uploadId,
		ObjectId: multipart.ObjectId,
	}, completeUpload)
	if err != nil {
		log.Errorln("failed to complete multipart. err:", err)
		return err
	}

	// get old object meta if it exist, this is not needed if versioning is enabled
	oldObj, err := s.MetaStorage.GetObject(ctx, bucketName, objectKey, "", false)
	if err != nil && err != ErrNoSuchKey {
		log.Errorf("get object[%s] failed, err:%v\n", objectKey, err)
		return ErrInternalError
	}
	log.Debugf("existObj=%v, err=%v\n", oldObj, err)

	// Add to objects table
	contentType := multipart.Metadata.ContentType
	object := &pb.Object{
		BucketName:       bucketName,
		ObjectKey:        objectKey,
		TenantId:         multipart.Metadata.TenantId,
		UserId:           multipart.Metadata.UserId,
		ContentType:      contentType,
		ObjectId:         multipart.ObjectId,
		LastModified:     time.Now().UTC().Unix(),
		Etag:             result.ETag,
		DeleteMarker:     false,
		CustomAttributes: multipart.Metadata.Attrs,
		Type:             ObjectTypeNormal,
		Tier:             utils.Tier1,
		Size:             result.Size,
		Location:         multipart.Metadata.Location,
	}
	var deleteObj *Object
	if oldObj != nil && oldObj.Location != object.Location {
		deleteObj = oldObj
	}
	err = s.MetaStorage.PutObject(ctx, &Object{Object: object}, deleteObj, &multipart, nil, true)
	if err != nil {
		log.Errorf("failed to put object meta[object:%+v, oldObj:%+v]. err:%v\n", object, oldObj, err)
		// TODO: consistent check & clean
		return ErrDBError
	}

	err = s.MetaStorage.DeleteMultipart(ctx, multipart)
	if err != nil {
		log.Errorln("failed to delete multipart. err:", err)
		return err
	}

	log.Infoln("CompleteMultipartUpload upload part successfully.")
	return nil
}

func (s *s3Service) AbortMultipartUpload(ctx context.Context, in *pb.AbortMultipartRequest, out *pb.BaseResponse) error {
	log.Info("AbortMultipartUpload is called in s3 service.")
	bucketName := in.BucketName
	objectKey := in.ObjectKey
	uploadId := in.UploadId

	var err error
	defer func() {
		out.ErrorCode = GetErrCode(err)
	}()

	isAdmin, tenantId, _, err := util.GetCredentialFromCtx(ctx)
	if err != nil && isAdmin == false {
		log.Error("get tenant id failed")
		err = ErrInternalError
		return err
	}

	bucket, err := s.MetaStorage.GetBucket(ctx, bucketName, true)
	if err != nil {
		log.Errorln("failed to get bucket from meta storage. err:", err)
		return err
	}

	if !isAdmin {
		switch bucket.Acl.CannedAcl {
		case "public-read-write":
			break
		default:
			if bucket.TenantId != tenantId {
				log.Errorln("bucket owner is not equal to request owner.")
				return ErrBucketAccessForbidden
			}
		}
	}

	multipart, err := s.MetaStorage.GetMultipart(bucketName, objectKey, uploadId)
	if err != nil {
		log.Errorln("failed to get multipart info. err:", err)
		return err
	}

	backendName := bucket.DefaultLocation
	backend, err := utils.GetBackend(ctx, s.backendClient, backendName)
	if err != nil {
		log.Errorln("failed to get backend client with err:", err)
		return err
	}
	sd, err := driver.CreateStorageDriver(backend.Type, backend)
	if err != nil {
		log.Errorln("failed to create storage. err:", err)
		return err
	}

	err = sd.AbortMultipartUpload(ctx, &pb.MultipartUpload{Bucket: bucketName, Key: objectKey, UploadId: uploadId, ObjectId: "objectId"})
	if err != nil {
		log.Errorln("failed to abort multipart. err:", err)
		return err
	}

	err = s.MetaStorage.DeleteMultipart(ctx, multipart)
	if err != nil {
		log.Errorln("failed to delete multipart. err:", err)
		return err
	}

	log.Infoln("Abort multipart successfully.")
	return nil
}

func (s *s3Service) ListObjectParts(ctx context.Context, in *pb.ListObjectPartsRequest, out *pb.ListObjectPartsResponse) error {
	log.Info("ListObjectParts is called in s3 service.")

	return nil
}
