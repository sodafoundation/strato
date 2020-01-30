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
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"strconv"
	"time"

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

var mapObjectKeyToVersionId map[string]string

func init() {
	mapObjectKeyToVersionId = make(map[string]string)
}

func addToMap(key string, versionid string) {
	mapObjectKeyToVersionId[key] = versionid
}

func getVersionId(key string) (versionid string) {
	return mapObjectKeyToVersionId[key]
}

func removeVersionId(key string) {
	delete(mapObjectKeyToVersionId, key)
}

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
	bucketName := in.BucketName
	objectKey := in.ObjectKey
	log.Infof("InitMultipartUpload is called in s3 service, bucketName=%s, objectKey=%s\n", bucketName, objectKey)

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

	isBucketVersioned := bucket.Versioning.Status == utils.VersioningEnabled
	if isBucketVersioned {
		log.Info("Inside service/object.go PUT bucket version enabled")
		versionId, _ := utils.GetVersionId()
		inputKey := objectKey
		// update the cloud backend object name here
		objectKey = objectKey + "_" + versionId
		addToMap(inputKey, versionId)
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

	backendName := in.Location
	if backendName == "" {
		backendName = bucket.DefaultLocation
	}
	// incase get backend failed
	ctx = utils.SetRepresentTenant(ctx, tenantId, bucket.TenantId)
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
	tier := in.Tier
	if tier == 0 {
		// if not set, use the default tier
		tier = utils.Tier1
	}
	res, err := sd.InitMultipartUpload(ctx, &pb.Object{BucketName: bucketName, ObjectKey: objectKey, Tier: tier})
	if err != nil {
		log.Errorln("failed to init multipart upload. err:", err)
		return err
	}

	multipartMetadata := MultipartMetadata{
		InitiatorId: tenantId,
		TenantId:    bucket.TenantId,
		UserId:      bucket.UserId,
		ContentType: contentType,
		Attrs:       attrs,
		Tier:        tier,
		Location:    backendName,
		// TODO: add sse information
	}
	if in.Acl != nil {
		multipartMetadata.Acl = *in.Acl
	} else {
		multipartMetadata.Acl.CannedAcl = "private"
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

	bucket, err := s.MetaStorage.GetBucket(ctx, bucketName, true)
	if err != nil {
		log.Errorln("get bucket failed with err:", err)
		return err
	}

	isBucketVersioned := bucket.Versioning.Status == utils.VersioningEnabled
	if isBucketVersioned {
		log.Info("Inside service/object.go PUT bucket version enabled")

		// update the cloud backend object name here
		objectKey = objectKey + "_" + getVersionId(objectKey)
	}

	log.Infof("uploadpart, bucketname:%v, objectkey:%v, partId:%v, uploadId:%v,size:%v", bucketName, objectKey, partId, uploadId, size)
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
	// incase get backend failed
	ctx = utils.SetRepresentTenant(ctx, tenantId, bucket.TenantId)
	backend, err := utils.GetBackend(ctx, s.backendClient, backendName)
	if err != nil {
		log.Errorln("failed to get backend client with err:", err)
		return err
	}

	md5Writer := md5.New()
	data := &StreamReader{in: stream}
	limitedDataReader := io.LimitReader(data, size)
	var dataReader io.Reader
	// encrypt if needed
	if bucket.ServerSideEncryption.SseType == "SSE" {

		var readerErr error
		tempReader, readerErr := utils.WrapEncryptionReader(limitedDataReader,
			bucket.ServerSideEncryption.EncryptionKey,
			bucket.ServerSideEncryption.InitilizationVector)
		if readerErr != nil {
			log.Errorln("failed to get encrypted reader with err:", readerErr)
			return readerErr
		}
		buf := new(bytes.Buffer)
		buf.ReadFrom(tempReader)
		size = int64(buf.Len())

		dataReader = io.TeeReader(io.LimitReader(bytes.NewReader(buf.Bytes()), size), md5Writer)
	} else {
		dataReader = io.TeeReader(limitedDataReader, md5Writer)
	}

	sd, err := driver.CreateStorageDriver(backend.Type, backend)
	if err != nil {
		log.Errorln("failed to create storage. err:", err)
		return nil
	}
	ctx = context.Background()
	ctx = context.WithValue(ctx, dscommon.CONTEXT_KEY_MD5, uploadRequest.Md5Hex)
	log.Infoln("bucketname:", bucketName, " objectKey:", objectKey, " uploadid:", uploadId, " objectId:", multipart.ObjectId, " partid:", partId)
	_, err = sd.UploadPart(ctx, dataReader, &pb.MultipartUpload{
		Bucket:   bucketName,
		Key:      objectKey,
		UploadId: uploadId,
		ObjectId: multipart.ObjectId},
		int64(partId), size)
	if err != nil {
		log.Errorln("failed to upload part to backend. err:", err)
		return err
	}

	calculatedMd5 := hex.EncodeToString(md5Writer.Sum(nil))
	part := Part{
		PartNumber:   int(partId),
		Size:         size,
		ObjectId:     multipart.ObjectId,
		Etag:         calculatedMd5,
		LastModified: time.Now().UTC().Format(CREATE_TIME_LAYOUT),
	}

	err = s.MetaStorage.PutObjectPart(ctx, multipart, part)
	if err != nil {
		log.Errorln("failed to put object part. err:", err)
		// because the backend will delete object part that has the same part id in next upload, we return error directly here
		return err
	}

	uploadResponse.ETag = calculatedMd5

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
	if err != nil {
		log.Error("get tenant id failed")
		err = ErrInternalError
		return err
	}

	bucket, err := s.MetaStorage.GetBucket(ctx, bucketName, true)
	if err != nil {
		log.Errorf("failed to get bucket from meta stoarge. err:", err)
		return err
	}

	isBucketVersioned := bucket.Versioning.Status == utils.VersioningEnabled
	if isBucketVersioned {
		log.Info("Inside service/object.go PUT bucket version enabled")
		inputKey := objectKey
		objectKey = objectKey + "_" + getVersionId(objectKey)
		removeVersionId(inputKey)

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

	md5Writer := md5.New()
	var totalSize int64 = 0
	for i := 0; i < len(in.CompleteParts); i++ {
		if in.CompleteParts[i].PartNumber != int64(i+1) {
			log.Errorln("wrong order for part number. ")
			err = ErrInvalidPart
			return err
		}
		part, ok := multipart.Parts[i+1]
		if !ok {
			log.Errorln("missed object part. partno:", i)
			err = ErrInvalidPart
			return err
		}

		if part.Etag != in.CompleteParts[i].ETag {
			log.Errorln("part etag in meta store is not the same with client's part, partno:", i)
			err = ErrInvalidPart
			return err
		}
		var etagBytes []byte
		etagBytes, err = hex.DecodeString(part.Etag)
		if err != nil {
			log.Errorln("failed to decode etag string. err:", err)
			err = ErrInvalidPart
			return err
		}
		part.Offset = totalSize
		totalSize += part.Size
		md5Writer.Write(etagBytes)
	}
	eTag := hex.EncodeToString(md5Writer.Sum(nil))
	eTag += "-" + strconv.Itoa(len(in.CompleteParts))

	backendName := multipart.Metadata.Location
	// incase get backend failed
	ctx = utils.SetRepresentTenant(ctx, tenantId, bucket.TenantId)
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
	_, mpuerr := sd.CompleteMultipartUpload(ctx, &pb.MultipartUpload{
		Bucket:   bucketName,
		Key:      objectKey,
		UploadId: uploadId,
		ObjectId: multipart.ObjectId,
		Location: multipart.Metadata.Location,
		Tier:     multipart.Metadata.Tier,
	}, completeUpload)

	if mpuerr != nil {
		log.Errorln("failed to complete multipart. err:", err)
		return mpuerr
	}

	// TODO: if versioning is enabled, not need to delete oldObj
	oldObj, err := s.MetaStorage.GetObject(ctx, bucketName, objectKey, "", false)
	if err != nil && err != ErrNoSuchKey {
		log.Errorf("get object[%s] failed, err:%v\n", objectKey, err)
		return ErrInternalError
	}
	log.Debugf("bucketName=%s,objectKey=%s,version=%s,existObj=%v, err=%v\n", bucketName, objectKey, in.SourceVersionID, oldObj, err)

	// Add to objects table
	contentType := multipart.Metadata.ContentType
	object := &pb.Object{
		BucketName:       bucketName,
		ObjectKey:        multipart.ObjectKey,
		TenantId:         multipart.Metadata.TenantId,
		UserId:           multipart.Metadata.UserId,
		ContentType:      contentType,
		ObjectId:         multipart.ObjectId,
		LastModified:     time.Now().UTC().Unix(),
		Etag:             eTag,
		DeleteMarker:     false,
		CustomAttributes: multipart.Metadata.Attrs,
		Type:             ObjectTypeNormal,
		Tier:             multipart.Metadata.Tier,
		Size:             totalSize,
		Location:         multipart.Metadata.Location,
		Acl:              &multipart.Metadata.Acl,
	}

	if in.RequestType != utils.RequestType_Lifecycle {
		err = s.MetaStorage.PutObject(ctx, &Object{Object: object}, oldObj, &multipart, nil, true)
		if err != nil {
			log.Errorf("failed to put object meta[object:%+v, oldObj:%+v]. err:%v\n", object, oldObj, err)
			// TODO: consistent check & clean
			return ErrDBError
		}
	} else {
		err = s.MetaStorage.UpdateObject4Lifecycle(ctx, oldObj, &Object{Object: object}, &multipart)
		if err != nil {
			log.Errorln("failed to put object meta. err:", err)
			// delete new object, lifecycle will try again in the next schedule round
			s.cleanObject(ctx, &Object{Object: object}, sd)
			return err
		} else {
			s.cleanObject(ctx, oldObj, nil)
		}
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

	backendName := multipart.Metadata.Location
	// incase get backend failed
	ctx = utils.SetRepresentTenant(ctx, tenantId, bucket.TenantId)
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

	err = sd.AbortMultipartUpload(ctx, &pb.MultipartUpload{
		Bucket:   bucketName,
		Key:      objectKey,
		UploadId: uploadId,
		ObjectId: multipart.ObjectId})
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
	var err error
	defer func() {
		out.ErrorCode = GetErrCode(err)
	}()

	bucketName := in.BucketName
	objectKey := in.ObjectKey
	uploadId := in.UploadId

	multipart, err := s.MetaStorage.GetMultipart(bucketName, objectKey, uploadId)
	if err != nil {
		log.Errorln("failed to get multipart info. err:", err)
		return err
	}

	isAdmin, tenantId, _, err := util.GetCredentialFromCtx(ctx)
	if err != nil {
		log.Error("get tenant id failed")
		err = ErrInternalError
		return err
	}

	if !isAdmin {
		switch multipart.Metadata.Acl.CannedAcl {
		case "public-read", "public-read-write":
			break
		default:
			if multipart.Metadata.TenantId != tenantId {
				err = ErrAccessDenied
				return err
			}
		}
	}

	out.Initiator = &pb.Owner{Id: multipart.Metadata.InitiatorId, DisplayName: multipart.Metadata.InitiatorId}
	out.Owner = &pb.Owner{Id: multipart.Metadata.TenantId, DisplayName: multipart.Metadata.TenantId}
	out.MaxParts = int64(in.MaxParts)
	out.Parts = make([]*pb.Part, 0)
	for i := in.PartNumberMarker + 1; i <= MAX_PART_NUMBER; i++ {
		if p, ok := multipart.Parts[int(i)]; ok {
			out.Parts = append(out.Parts, &pb.Part{
				PartNumber:   i,
				ETag:         "\"" + p.Etag + "\"",
				Size:         p.Size,
				LastModified: p.LastModified,
			})

			if int64(len(out.Parts)) > in.MaxParts {
				break
			}
		}
	}
	if int64(len(out.Parts)) == in.MaxParts+1 {
		out.IsTruncated = true
		out.NextPartNumberMarker = out.Parts[out.MaxParts].PartNumber
		out.Parts = out.Parts[:in.MaxParts]
	}
	out.PartNumberMarker = in.PartNumberMarker

	log.Infof("list object part successfully. ")

	return nil
}

func (s *s3Service) CopyObjPart(ctx context.Context, in *pb.CopyObjPartRequest, out *pb.CopyObjPartResponse) error {
	log.Info("CopyObjPart is called in s3 service.")
	var err error
	defer func() {
		out.ErrorCode = GetErrCode(err)
	}()

	srcBucketName := in.SourceBucket
	srcObjectName := in.SourceObject
	targetBucketName := in.TargetBucket
	targetObjectName := in.TargetObject
	uploadId := in.UploadID
	partId := in.PartID
	size := in.ReadLength
	offset := in.ReadOffset

	srcBucket, err := s.MetaStorage.GetBucket(ctx, srcBucketName, true)
	if err != nil {
		log.Errorln("get bucket failed with err:", err)
		return err
	}
	srcObject, err := s.MetaStorage.GetObject(ctx, srcBucketName, srcObjectName, "", true)
	if err != nil {
		log.Errorln("failed to get object info from meta storage. err:", err)
		return err
	}
	targetBucket, err := s.MetaStorage.GetBucket(ctx, targetBucketName, true)
	if err != nil {
		log.Errorln("get bucket failed with err:", err)
		return err
	}

	isAdmin, tenantId, _, err := util.GetCredentialFromCtx(ctx)
	if err != nil {
		log.Error("get tenant id failed, err:", err)
		err = ErrInternalError
		return err
	}

	if !isAdmin {
		//check source object acl
		switch srcObject.Acl.CannedAcl {
		case "public-read", "public-read-write":
			break
		default:
			if srcObject.TenantId != tenantId {
				err = ErrAccessDenied
				return err
			}
		}
		//check target acl
		switch targetBucket.Acl.CannedAcl {
		case "public-read-write":
			break
		default:
			if targetBucket.TenantId != tenantId {
				err = ErrBucketAccessForbidden
				return err
			}
		} // TODO policy and fancy ACL
	}

	backendName := srcBucket.DefaultLocation
	if srcObject.Location != "" {
		backendName = srcObject.Location
	}
	srcBackend, err := utils.GetBackend(ctx, s.backendClient, backendName)
	if err != nil {
		log.Errorln("failed to get backend client with err:", err)
		return err
	}
	srcSd, err := driver.CreateStorageDriver(srcBackend.Type, srcBackend)
	if err != nil {
		log.Errorln("failed to create storage driver. err:", err)
		return err
	}
	targetBackendName := targetBucket.DefaultLocation
	if in.TargetLocation != "" {
		targetBackendName = in.TargetLocation
	}
	// incase get backend failed
	ctx = utils.SetRepresentTenant(ctx, tenantId, targetBucket.TenantId)
	targetBackend, err := utils.GetBackend(ctx, s.backendClient, targetBackendName)
	if err != nil {
		log.Errorln("failed to get backend client with err:", err)
		return err
	}
	targetSd, err := driver.CreateStorageDriver(targetBackend.Type, targetBackend)
	if err != nil {
		log.Errorln("failed to create storage driver. err:", err)
		return err
	}
	reader, err := srcSd.Get(ctx, srcObject.Object, offset, offset+size-1)
	if err != nil {
		log.Errorln("failed to get data reader. err:", err)
		return err
	}
	limitedDataReader := io.LimitReader(reader, size)
	md5Writer := md5.New()
	dataReader := io.TeeReader(limitedDataReader, md5Writer)

	multipart, err := s.MetaStorage.GetMultipart(targetBucketName, targetObjectName, uploadId)
	if err != nil {
		log.Errorln("failed to get multipart. err:", err)
		return err
	}
	_, err = targetSd.UploadPart(ctx, dataReader, &pb.MultipartUpload{
		Bucket:   targetBucketName,
		Key:      targetObjectName,
		UploadId: uploadId,
		ObjectId: multipart.ObjectId},
		partId, size)
	if err != nil {
		log.Errorln("failed to upload part to backend. err:", err)
		return err
	}
	calculatedMd5 := hex.EncodeToString(md5Writer.Sum(nil))
	part := Part{
		PartNumber:   int(partId),
		Size:         size,
		ObjectId:     multipart.ObjectId,
		Etag:         calculatedMd5,
		LastModified: time.Now().UTC().Format(CREATE_TIME_LAYOUT),
	}

	err = s.MetaStorage.PutObjectPart(ctx, multipart, part)
	if err != nil {
		log.Errorln("failed to put object part. err:", err)
		// because the backend will delete object part that has the same part id in next upload, we return error directly here
		return err
	}

	out.Etag = calculatedMd5
	out.LastModified = time.Now().UTC().Unix()

	log.Infoln("copy object part successfully.")
	return nil
}
