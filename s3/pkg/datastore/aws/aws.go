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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/opensds/multi-cloud/api/pkg/utils/constants"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	. "github.com/opensds/multi-cloud/s3/error"
	dscommon "github.com/opensds/multi-cloud/s3/pkg/datastore/common"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

type AwsAdapter struct {
	backend *backendpb.BackendDetail
	session *session.Session
}

type s3Cred struct {
	ak string
	sk string
}

func (myc *s3Cred) Retrieve() (credentials.Value, error) {
	cred := credentials.Value{AccessKeyID: myc.ak, SecretAccessKey: myc.sk}
	return cred, nil
}

func (myc *s3Cred) IsExpired() bool {
	return false
}

func (ad *AwsAdapter) Put(ctx context.Context, stream io.Reader, object *pb.Object) (dscommon.PutResult, error) {
	bucket := ad.backend.BucketName
	objectId := object.BucketName + "/" + object.ObjectKey
	result := dscommon.PutResult{}

	uploader := s3manager.NewUploader(ad.session)
	log.Infof("put object[AWS S3] begin, objectId:%s\n", objectId)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: &bucket,
		Key:    &objectId,
		Body:   stream,
		// Currently, only support STANDARD for PUT.
		StorageClass: aws.String(constants.StorageClassAWSStandard),
	})
	log.Infof("put object[AWS S3] end, objectId:%s\n", objectId)
	if err != nil {
		log.Errorf("put object[AWS S3] failed, objectId:%s, err:%v", objectId, err)
		return result, ErrPutToBackendFailed
	}

	result.UpdateTime = time.Now().Unix()
	result.ObjectId = objectId
	// TODO: set ETAG
	log.Infof("put object[AWS S3] successfully, objectId:%s, UpdateTime is:%v\n", objectId, result.UpdateTime)

	return result, nil
}

func (ad *AwsAdapter) Get(ctx context.Context, object *pb.Object, start int64, end int64) (io.ReadCloser, error) {
	bucket := ad.backend.BucketName
	var buf []byte
	writer := aws.NewWriteAtBuffer(buf)
	objectId := object.BucketName + "/" + object.ObjectKey
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

	downloader := s3manager.NewDownloader(ad.session)
	numBytes, err := downloader.DownloadWithContext(ctx, writer, &getObjectInput)
	if err != nil {
		log.Errorf("get object[AWS S3] failed, objectId:%s, err:%v", objectId, err)
		return nil, ErrGetFromBackendFailed
	}

	log.Infof("get object[AWS S3] succeed, objectId:%s, numBytes:%d\n", objectId, numBytes)
	log.Infof("writer.Bytes() is %v \n", writer.Bytes())
	body := bytes.NewReader(writer.Bytes())
	ioReaderClose := ioutil.NopCloser(body)

	return ioReaderClose, nil
}

func (ad *AwsAdapter) Delete(ctx context.Context, input *pb.DeleteObjectInput) error {
	bucket := ad.backend.BucketName
	objectId := input.Bucket + "/" + input.Key
	deleteInput := awss3.DeleteObjectInput{Bucket: &bucket, Key: &objectId}

	log.Infof("delete object[AWS S3], objectId:%s.\n", objectId)
	svc := awss3.New(ad.session)
	_, err := svc.DeleteObject(&deleteInput)
	if err != nil {
		log.Errorf("delete object[AWS S3] failed, objectId:%s, err:%v.\n", objectId, err)
		return ErrDeleteFromBackendFailed
	}

	log.Infof("delete object[AWS S3] succeed, objectId:%s.\n", objectId)

	return nil
}

func (ad *AwsAdapter) Copy(ctx context.Context, stream io.Reader, target *pb.Object) (result dscommon.PutResult, err error) {
	return
}

func (ad *AwsAdapter) InitMultipartUpload(ctx context.Context, object *pb.Object) (*pb.MultipartUpload, error) {
	bucket := ad.backend.BucketName
	objectId := object.BucketName + "/" + object.ObjectKey
	log.Infof("init multipart upload[AWS S3], bucket = %v,objectId = %v\n", bucket, objectId)
	multipartUpload := &pb.MultipartUpload{}
	multiUpInput := &awss3.CreateMultipartUploadInput{
		Bucket: &bucket,
		Key:    &objectId,
		// Currently, only support STANDARD.
		StorageClass: aws.String(constants.StorageClassAWSStandard),
	}

	svc := awss3.New(ad.session)
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
	tries := 1
	bucket := ad.backend.BucketName
	bytess, _ := ioutil.ReadAll(stream)
	upPartInput := &awss3.UploadPartInput{
		Body:          bytes.NewReader(bytess),
		Bucket:        &bucket,
		Key:           &multipartUpload.ObjectId,
		PartNumber:    aws.Int64(partNumber),
		UploadId:      &multipartUpload.UploadId,
		ContentLength: aws.Int64(upBytes),
	}
	log.Infof("upload part[AWS S3], input:%v\n", upPartInput)

	svc := awss3.New(ad.session)
	for tries <= 3 {
		upRes, err := svc.UploadPart(upPartInput)
		if err != nil {
			if tries == 3 {
				log.Errorf("upload part[AWS S3] failed. err:%v\n", err)
				return nil, ErrPutToBackendFailed
			}
			log.Debugf("retrying to upload[AWS S3] part#%d ,err:%s\n", partNumber, err)
			tries++
		} else {
			log.Infof("upload object[AWS S3], objectId:%s, part #%d succeed, ETag:%s\n", multipartUpload.ObjectId,
				partNumber, *upRes.ETag)
			result := &model.UploadPartResult{
				Xmlns:      model.Xmlns,
				ETag:       *upRes.ETag,
				PartNumber: partNumber}
			return result, nil
		}
	}

	log.Error("upload part[AWS S3]: should not be here.")
	return nil, ErrInternalError
}

func (ad *AwsAdapter) CompleteMultipartUpload(ctx context.Context, multipartUpload *pb.MultipartUpload,
	completeUpload *model.CompleteMultipartUpload) (*model.CompleteMultipartUploadResult, error) {
	bucket := ad.backend.BucketName
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

	log.Infof("completeInput %v\n", completeInput)
	svc := awss3.New(ad.session)
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
	bucket := ad.backend.BucketName
	log.Infof("abort multipart upload[AWS S3], bucket:%s, objectId:%s.\n", bucket, multipartUpload.ObjectId)

	abortInput := &awss3.AbortMultipartUploadInput{
		Bucket:   &bucket,
		Key:      &multipartUpload.ObjectId,
		UploadId: &multipartUpload.UploadId,
	}

	svc := awss3.New(ad.session)
	rsp, err := svc.AbortMultipartUpload(abortInput)
	if err != nil {
		log.Errorf("abort multipart upload[AWS S3] failed, err:%v\n", err)
		return ErrBackendAbortMultipartFailed
	}

	log.Infof("complete multipart upload[AWS S3] successfully, rsp:%v\n", rsp)
	return nil
}

func (ad *AwsAdapter) ListParts(ctx context.Context, multipartUpload *pb.ListParts) (*model.ListPartsOutput, error) {
	return nil, errors.New("not implemented yet.")
}

func (ad *AwsAdapter) Close() error {
	// TODO:
	return nil
}
