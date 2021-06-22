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

package aws

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"strconv"
	"time"

	"crypto/md5"
	"encoding/base64"
	"encoding/hex"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"

	backendpb "github.com/opensds/multi-cloud/backend/proto"
	. "github.com/opensds/multi-cloud/s3/error"
	dscommon "github.com/opensds/multi-cloud/s3/pkg/datastore/common"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	osdss3 "github.com/opensds/multi-cloud/s3/pkg/service"
	"github.com/opensds/multi-cloud/s3/pkg/utils"
	pb "github.com/opensds/multi-cloud/s3/proto"
)

const sampleBucket string = "sample"

type AwsAdapter struct {
	Backend *backendpb.BackendDetail
	Session *session.Session
}

type S3Cred struct {
	Ak string
	Sk string
}

func (myc *S3Cred) Retrieve() (credentials.Value, error) {
	cred := credentials.Value{AccessKeyID: myc.Ak, SecretAccessKey: myc.Sk}
	return cred, nil
}

func (myc *S3Cred) IsExpired() bool {
	return false
}

func (ad *AwsAdapter) BucketCreate(ctx context.Context, in *pb.Bucket) error {

	log.Info("Bucket create is called in aws service and input request is:", in)
	svc := awss3.New(ad.Session)

	input := &awss3.CreateBucketInput{
		Bucket: aws.String(in.Name),
	}

	buckout, err := svc.CreateBucket(input)
	if err != nil {
		log.Error("the create bucket failed in aws-s3 service with err:%s", err.Error())
		return err
	}
	log.Debug("The bucket creation successful in aws-s3 service with output:%s", buckout)

	return nil
}

func (ad *AwsAdapter) Put(ctx context.Context, stream io.Reader, object *pb.Object) (dscommon.PutResult, error) {
	log.Infof("Aws-s3 service called to put data with stream=[%s] and object=[%s]", stream, object)
	bucket := object.BucketName
	objectId := object.ObjectKey
	result := dscommon.PutResult{}
	userMd5 := dscommon.GetMd5FromCtx(ctx)
	size := object.Size
	storageClass := dscommon.GetStorClassFromCtx(ctx)
	log.Infof("put object, objectId:%s, bucket:%s, size=%d, storageClass=%s userMd5=%s", objectId, bucket, size, storageClass, userMd5)

	// Limit the reader to its provided size if specified.
	var limitedDataReader io.Reader
	if size > 0 { // request.ContentLength is -1 if length is unknown
		limitedDataReader = io.LimitReader(stream, size)
	} else {
		limitedDataReader = stream
	}
	md5Writer := md5.New()
	dataReader := io.TeeReader(limitedDataReader, md5Writer)

	// If the Storage Class is defined from the API request Header, set it else define defaults
	var storClass string
	var err error

	if storageClass != "" {
		storClass = storageClass
	} else {
		if object.Tier == 0 {
			// default
			object.Tier = utils.Tier1
		}
		storClass, err = osdss3.GetNameFromTier(object.Tier, utils.OSTYPE_AWS)
		if err != nil {
			log.Infof("translate tier[%d] to aws storage class failed", object.Tier)
			return result, ErrInternalError
		}
	}

	uploader := s3manager.NewUploader(ad.Session)
	input := &s3manager.UploadInput{
		Body:         dataReader,
		Bucket:       aws.String(bucket),
		Key:          aws.String(objectId),
		StorageClass: aws.String(storClass),
	}
	if userMd5 != "" {
		md5Bytes, err := hex.DecodeString(userMd5)
		if err != nil {
			log.Warnf("user input md5 is abandoned, cause decode md5 failed, err:%v\n", err)
		} else {
			input.ContentMD5 = aws.String(base64.StdEncoding.EncodeToString(md5Bytes))
			log.Debugf("input.ContentMD5=%s\n", *input.ContentMD5)
		}
	}
	log.Infof("upload object[AWS S3] start, objectId:%s\n", objectId)
	ret, err := uploader.Upload(input)
	if err != nil {
		log.Errorf("put object[AWS S3] failed, objectId:%s, err:%v\n", objectId, err)
		return result, ErrPutToBackendFailed
	}
	log.Infof("put object[AWS S3] end, objectId:%s\n", objectId)

	calculatedMd5 := hex.EncodeToString(md5Writer.Sum(nil))
	log.Debug("calculatedMd5:", calculatedMd5, ", userMd5:", userMd5)
	if userMd5 != "" && userMd5 != calculatedMd5 {
		log.Error("### MD5 not match, calculatedMd5:", calculatedMd5, ", userMd5:", userMd5)
		return result, ErrBadDigest
	}

	if ret.VersionID != nil {
		result.Meta = *ret.VersionID
	}
	result.UpdateTime = time.Now().Unix()
	result.ObjectId = objectId
	result.Etag = calculatedMd5
	result.Written = size
	log.Infof("put object[AWS S3] successfully, objectId:%s, UpdateTime is:%v\n", objectId, result.UpdateTime)

	return result, nil
}

func (ad *AwsAdapter) Get(ctx context.Context, object *pb.Object, start int64, end int64) (io.ReadCloser, error) {
	bucket := object.BucketName
	objectId := object.ObjectId
	getObjectInput := awss3.GetObjectInput{
		Bucket: &bucket,
		Key:    &objectId,
	}
	log.Infof("get object[AWS S3], objectId:%s, start = %d, end = %d\n", objectId, start, end)
	if start != 0 || end != 0 {
		strStart := strconv.FormatInt(start, 10)
		strEnd := strconv.FormatInt(end, 10)
		rangestr := "bytes=" + strStart + "-" + strEnd
		getObjectInput.SetRange(rangestr)
	}

	svc := awss3.New(ad.Session)
	result, err := svc.GetObject(&getObjectInput)
	if err != nil {
		log.Errorf("get object[AWS S3] failed, objectId:%s, err:%v", objectId, err)
		return nil, ErrGetFromBackendFailed
	}

	log.Infof("get object[AWS S3] succeed, objectId:%s, ContentLength:%d\n", objectId, *result.ContentLength)
	return result.Body, nil
}

func (ad *AwsAdapter) BucketDelete(ctx context.Context, in *pb.Bucket) error {
	log.Info("Bucket delete is called in aws s3 service")
	svc := awss3.New(ad.Session)
	input := &awss3.DeleteBucketInput{
		Bucket: aws.String(in.Name),
	}

	result, err := svc.DeleteBucketWithContext(ctx, input)
	if err != nil {
		log.Error(err.Error())
		return err
	}
	log.Debug(result)

	return nil
}

func (ad *AwsAdapter) Delete(ctx context.Context, input *pb.DeleteObjectInput) error {
	log.Info("Bucket object delete is called in aws s3 service")
	bucket := input.Bucket
	objectId := input.Key
	log.Info("Deleting the object of bucket:%s and object:%s", bucket, objectId)
	deleteInput := awss3.DeleteObjectInput{Bucket: &bucket, Key: &objectId}

	log.Infof("delete object[AWS S3], objectId:%s.\n", objectId)

	svc := awss3.New(ad.Session)
	_, err := svc.DeleteObject(&deleteInput)
	if err != nil {
		log.Errorf("delete object[AWS S3] failed, objectId:%s, err:%v.\n", objectId, err)
		return ErrDeleteFromBackendFailed
	}

	log.Infof("delete object[AWS S3] succeed, objectId:%s.\n", objectId)

	return nil
}

func (ad *AwsAdapter) Copy(ctx context.Context, stream io.Reader, target *pb.Object) (result dscommon.PutResult, err error) {
	log.Errorf("copy[AWS S3] is not supported.")
	err = ErrInternalError
	return
}

func (ad *AwsAdapter) ChangeStorageClass(ctx context.Context, object *pb.Object, newClass *string) error {
	objectId := object.ObjectId
	log.Infof("change storage class[AWS S3] of object[%s] to %s .\n", object, *newClass)

	svc := awss3.New(ad.Session)
	input := &awss3.CopyObjectInput{
		Bucket:     aws.String(object.BucketName),
		Key:        aws.String(objectId),
		CopySource: aws.String(object.BucketName + "/" + objectId),
	}
	input.StorageClass = aws.String(*newClass)
	_, err := svc.CopyObject(input)
	if err != nil {
		log.Errorf("change storage class[AWS S3] of object[%s] to %s failed: %v.\n", objectId, *newClass, err)
		return ErrPutToBackendFailed
	}

	log.Infof("change storage class[AWS S3] of object[%s] to %s succeed.\n", objectId, *newClass)
	return nil
}

func (ad *AwsAdapter) InitMultipartUpload(ctx context.Context, object *pb.Object) (*pb.MultipartUpload, error) {
	bucket := object.BucketName
	objectId := object.ObjectKey
	log.Infof("init multipart upload[AWS S3], bucket = %v,objectId = %v\n", bucket, objectId)

	storClass, err := osdss3.GetNameFromTier(object.Tier, utils.OSTYPE_AWS)
	if err != nil {
		log.Warnf("translate tier[%d] to aws storage class failed, use default value.\n", object.Tier)
		return nil, ErrInternalError
	}

	multipartUpload := &pb.MultipartUpload{}
	multiUpInput := &awss3.CreateMultipartUploadInput{
		Bucket:       &bucket,
		Key:          &objectId,
		StorageClass: aws.String(storClass),
	}

	svc := awss3.New(ad.Session)
	res, err := svc.CreateMultipartUpload(multiUpInput)
	if err != nil {
		log.Fatalf("init multipart upload[AWS S3] failed, err:%v\n", err)
		return nil, ErrBackendInitMultipartFailed
	} else {
		log.Infof("init multipart upload[AWS S3] succeed, UploadId:%s\n", *res.UploadId)
		multipartUpload.Bucket = object.BucketName
		multipartUpload.Key = object.ObjectKey
		multipartUpload.UploadId = *res.UploadId
		multipartUpload.ObjectId = objectId
		return multipartUpload, nil
	}
}

func (ad *AwsAdapter) UploadPart(ctx context.Context, stream io.Reader, multipartUpload *pb.MultipartUpload,
	partNumber int64, upBytes int64) (*model.UploadPartResult, error) {
	bucket := multipartUpload.Bucket
	bytess, err := ioutil.ReadAll(stream)
	if err != nil {
		log.Errorf("read data failed, err:%v\n", err)
		return nil, ErrInternalError
	}
	upPartInput := &awss3.UploadPartInput{
		Body:          bytes.NewReader(bytess),
		Bucket:        &bucket,
		Key:           &multipartUpload.ObjectId,
		PartNumber:    aws.Int64(partNumber),
		UploadId:      &multipartUpload.UploadId,
		ContentLength: aws.Int64(upBytes),
	}
	log.Infof("upload part[AWS S3], input:%v\n", *upPartInput)

	svc := awss3.New(ad.Session)
	upRes, err := svc.UploadPart(upPartInput)
	if err != nil {
		log.Errorf("upload part[AWS S3] failed. err:%v\n", err)
		return nil, ErrPutToBackendFailed
	} else {
		log.Infof("upload object[AWS S3], objectId:%s, part #%d succeed, ETag:%s\n", multipartUpload.ObjectId,
			partNumber, *upRes.ETag)
		result := &model.UploadPartResult{
			Xmlns:      model.Xmlns,
			ETag:       *upRes.ETag,
			PartNumber: partNumber}
		return result, nil
	}

	log.Error("upload part[AWS S3]: should not be here.")
	return nil, ErrInternalError
}

func (ad *AwsAdapter) CompleteMultipartUpload(ctx context.Context, multipartUpload *pb.MultipartUpload,
	completeUpload *model.CompleteMultipartUpload) (*model.CompleteMultipartUploadResult, error) {
	bucket := multipartUpload.Bucket
	log.Infof("complete multipart upload[AWS S3], bucket:%s, objectId:%s.\n", bucket, multipartUpload.ObjectId)

	var completeParts []*awss3.CompletedPart
	for _, p := range completeUpload.Parts {
		completePart := &awss3.CompletedPart{
			ETag:       aws.String(p.ETag),
			PartNumber: aws.Int64(p.PartNumber),
		}
		completeParts = append(completeParts, completePart)
	}
	completeInput := &awss3.CompleteMultipartUploadInput{
		Bucket:   &bucket,
		Key:      &multipartUpload.ObjectId,
		UploadId: &multipartUpload.UploadId,
		MultipartUpload: &awss3.CompletedMultipartUpload{
			Parts: completeParts,
		},
	}

	log.Infof("completeInput %v\n", *completeInput)
	svc := awss3.New(ad.Session)
	resp, err := svc.CompleteMultipartUpload(completeInput)
	if err != nil {
		log.Errorf("complete multipart upload[AWS S3] failed, err:%v\n", err)
		return nil, ErrBackendCompleteMultipartFailed
	}
	result := &model.CompleteMultipartUploadResult{
		Xmlns:    model.Xmlns,
		Location: *resp.Location,
		Bucket:   multipartUpload.Bucket,
		Key:      multipartUpload.Key,
		ETag:     *resp.ETag,
	}

	log.Infof("complete multipart upload[AWS S3] successfully, resp:%v\n", resp)
	return result, nil
}

func (ad *AwsAdapter) AbortMultipartUpload(ctx context.Context, multipartUpload *pb.MultipartUpload) error {
	bucket := multipartUpload.Bucket
	log.Infof("abort multipart upload[AWS S3], bucket:%s, objectId:%s.\n", bucket, multipartUpload.ObjectId)

	abortInput := &awss3.AbortMultipartUploadInput{
		Bucket:   &bucket,
		Key:      &multipartUpload.ObjectId,
		UploadId: &multipartUpload.UploadId,
	}

	svc := awss3.New(ad.Session)
	rsp, err := svc.AbortMultipartUpload(abortInput)
	if err != nil {
		log.Errorf("abort multipart upload[AWS S3] failed, err:%v\n", err)
		return ErrBackendAbortMultipartFailed
	}

	log.Infof("abort multipart upload[AWS S3] successfully, rsp:%v\n", rsp)
	return nil
}

func (ad *AwsAdapter) Restore(ctx context.Context, input *pb.Restore) error {
	bucket := input.BucketName
	objectId := input.ObjectKey
	resInput := &awss3.RestoreObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectId),
		RestoreRequest: &awss3.RestoreRequest{
			Days: aws.Int64(input.Days),
			GlacierJobParameters: &awss3.GlacierJobParameters{
				Tier: aws.String(input.Tier),
			},
		},
	}

	svc := awss3.New(ad.Session)
	result, err := svc.RestoreObject(resInput)
	if err != nil {
		log.Errorf("error while restoring object in AWS bucket: [%v]", err)
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case awss3.ErrCodeObjectAlreadyInActiveTierError:
				return ErrRestoreObjectFailed
			default:
				return ErrRestoreInProgress
			}
		} else {
			return ErrRestoreObjectFailed
		}
	}
	log.Infof("Result returned by Restore object [%v]", result)
	return nil

}

func (ad *AwsAdapter) ListParts(ctx context.Context, multipartUpload *pb.ListParts) (*model.ListPartsOutput, error) {
	return nil, errors.New("not implemented yet.")
}

func (ad *AwsAdapter) BackendCheck(ctx context.Context, backendDetail *pb.BackendDetailS3) error {
	randId := uuid.NewV4().String()
	input := &pb.Bucket{
		Name: sampleBucket + randId,
	}

	err := ad.BucketCreate(ctx, input)
	if err != nil {
		log.Error("failed to create sample bucket :", err)
		return err
	}

	log.Debug("Create sample bucket is successul")
	err = ad.BucketDelete(ctx, input)
	if err != nil {
		log.Error("failed to delete sample bucket :", err)
		return err
	}

	log.Debug("Delete sample bucket is successful")
	return nil
}

func (ad *AwsAdapter) Close() error {
	// TODO:
	return nil
}
