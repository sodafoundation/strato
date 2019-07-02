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
	"io"
	"io/ioutil"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/micro/go-log"

	"github.com/opensds/multi-cloud/api/pkg/utils/constants"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	pb "github.com/opensds/multi-cloud/s3/proto"
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

func Init(backend *backendpb.BackendDetail) *AwsAdapter {
	endpoint := backend.Endpoint
	AccessKeyID := backend.Access
	AccessKeySecret := backend.Security
	region := backend.Region

	s3aksk := s3Cred{ak: AccessKeyID, sk: AccessKeySecret}
	creds := credentials.NewCredentials(&s3aksk)

	disableSSL := true
	sess, err := session.NewSession(&aws.Config{
		Region:      &region,
		Endpoint:    &endpoint,
		Credentials: creds,
		DisableSSL:  &disableSSL,
	})
	if err != nil {
		return nil
	}

	adap := &AwsAdapter{backend: backend, session: sess}
	return adap
}

func (ad *AwsAdapter) PUT(stream io.Reader, object *pb.Object, ctx context.Context) S3Error {
	bucket := ad.backend.BucketName

	newObjectKey := object.BucketName + "/" + object.ObjectKey

	if ctx.Value("operation") == "upload" {
		uploader := s3manager.NewUploader(ad.session)
		_, err := uploader.Upload(&s3manager.UploadInput{
			Bucket: &bucket,
			Key:    &newObjectKey,
			Body:   stream,
			// Currently, only support STANDARD for PUT.
			StorageClass: aws.String(constants.StorageClassAWSStandard),
		})

		if err != nil {
			log.Logf("Upload to aws failed:%v", err)
			return S3Error{Code: 500, Description: "Upload to aws failed"}
		} else {
			object.LastModified = time.Now().Unix()
			log.Logf("LastModified is:%v\n", object.LastModified)
		}

	}

	return NoError
}

func (ad *AwsAdapter) GET(object *pb.Object, context context.Context, start int64, end int64) (io.ReadCloser, S3Error) {

	bucket := ad.backend.BucketName
	var buf []byte
	writer := aws.NewWriteAtBuffer(buf)
	newObjectKey := object.BucketName + "/" + object.ObjectKey
	getObjectInput := awss3.GetObjectInput{
		Bucket: &bucket,
		Key:    &newObjectKey,
	}
	log.Logf("start = %d, end = %d\n", start, end)
	if start != 0 || end != 0 {
		strStart := strconv.FormatInt(start, 10)
		strEnd := strconv.FormatInt(end, 10)
		rangestr := "bytes=" + strStart + "-" + strEnd
		getObjectInput.SetRange(rangestr)
	}

	if context.Value("operation") == "download" {
		downloader := s3manager.NewDownloader(ad.session)
		numBytes, err := downloader.DownloadWithContext(context, writer, &getObjectInput)
		//numBytes,err:=downloader.Download(writer,&getObjectInput)
		if err != nil {
			log.Logf("Download failed:%v", err)
			return nil, S3Error{Code: 500, Description: "Download failed"}
		} else {
			log.Logf("Download succeed, bytes:%d\n", numBytes)
			//log.Logf("writer.Bytes() is %v \n",writer.Bytes())
			body := bytes.NewReader(writer.Bytes())
			ioReaderClose := ioutil.NopCloser(body)
			return ioReaderClose, NoError
		}

	}

	return nil, NoError
}

func (ad *AwsAdapter) DELETE(object *pb.DeleteObjectInput, ctx context.Context) S3Error {

	bucket := ad.backend.BucketName

	newObjectKey := object.Bucket + "/" + object.Key

	deleteInput := awss3.DeleteObjectInput{Bucket: &bucket, Key: &newObjectKey}

	svc := awss3.New(ad.session)
	_, err := svc.DeleteObject(&deleteInput)
	if err != nil {
		log.Logf("Delete object failed, err:%v\n", err)
		return InternalError
	}

	log.Logf("Delete object %s from aws successfully.\n", newObjectKey)

	return NoError
}

func (ad *AwsAdapter) RestoreObject(object *pb.RestoreObjectInput, ctx context.Context) S3Error {

	bucket := ad.backend.BucketName

	newObjectKey := object.Bucket + "/" + object.Key

	restoreInput := awss3.RestoreObjectInput{Bucket: &bucket, Key: &newObjectKey}

	svc := awss3.New(ad.session)
	_, err := svc.RestoreObject(&restoreInput)
	if err != nil {
		log.Logf("restore object failed, err:%v\n", err)
		return InternalError
	}

	log.Logf("restore object %s from aws successfully.\n", newObjectKey)

	return NoError
}

func (ad *AwsAdapter) GetObjectInfo(bucketName string, key string, context context.Context) (*pb.Object, S3Error) {
	bucket := ad.backend.BucketName
	newKey := bucketName + "/" + key

	input := &awss3.ListObjectsInput{
		Bucket: &bucket,
		Prefix: &newKey,
	}

	svc := awss3.New(ad.session)
	output, err := svc.ListObjects(input)
	if err != nil {
		log.Fatalf("Init s3 multipart upload failed, err:%v\n", err)
		return nil, S3Error{Code: 500, Description: err.Error()}
	}

	for _, content := range output.Contents {
		realKey := bucketName + "/" + key
		if realKey != *content.Key {
			break
		}
		obj := &pb.Object{
			BucketName: bucketName,
			ObjectKey:  key,
			Size:       *content.Size,
		}
		return obj, NoError
	}
	log.Logf("Can not find spceified object(%s).\n", key)
	return nil, NoSuchObject
}

func (ad *AwsAdapter) InitMultipartUpload(object *pb.Object, context context.Context) (*pb.MultipartUpload, S3Error) {
	bucket := ad.backend.BucketName
	newObjectKey := object.BucketName + "/" + object.ObjectKey
	log.Logf("bucket = %v,newObjectKey = %v\n", bucket, newObjectKey)
	multipartUpload := &pb.MultipartUpload{}
	multiUpInput := &awss3.CreateMultipartUploadInput{
		Bucket: &bucket,
		Key:    &newObjectKey,
		// Currently, only support STANDARD.
		StorageClass: aws.String(constants.StorageClassAWSStandard),
	}

	svc := awss3.New(ad.session)
	res, err := svc.CreateMultipartUpload(multiUpInput)
	if err != nil {
		log.Fatalf("Init s3 multipart upload failed, err:%v\n", err)
		return nil, S3Error{Code: 500, Description: err.Error()}
	} else {
		log.Logf("Init s3 multipart upload succeed, UploadId:%s\n", *res.UploadId)
		multipartUpload.Bucket = object.BucketName
		multipartUpload.Key = object.ObjectKey
		multipartUpload.UploadId = *res.UploadId
		return multipartUpload, NoError
	}
}

func (ad *AwsAdapter) UploadPart(stream io.Reader,
	multipartUpload *pb.MultipartUpload,
	partNumber int64, upBytes int64,
	context context.Context) (*model.UploadPartResult, S3Error) {
	tries := 1
	bucket := ad.backend.BucketName
	newObjectKey := multipartUpload.Bucket + "/" + multipartUpload.Key
	bytess, _ := ioutil.ReadAll(stream)
	upPartInput := &awss3.UploadPartInput{
		Body:          bytes.NewReader(bytess),
		Bucket:        &bucket,
		Key:           &newObjectKey,
		PartNumber:    aws.Int64(partNumber),
		UploadId:      &multipartUpload.UploadId,
		ContentLength: aws.Int64(upBytes),
	}
	log.Logf(">>>%v", upPartInput)

	svc := awss3.New(ad.session)
	for tries <= 3 {

		upRes, err := svc.UploadPart(upPartInput)
		if err != nil {
			if tries == 3 {
				log.Logf("[ERROR]Upload part to aws failed. err:%v\n", err)
				return nil, S3Error{Code: 500, Description: "Upload failed"}
			}
			log.Logf("Retrying to upload part#%d ,err:%s\n", partNumber, err)
			tries++
		} else {
			log.Logf("Uploaded part #%d, ETag:%s\n", partNumber, *upRes.ETag)
			result := &model.UploadPartResult{
				Xmlns:      model.Xmlns,
				ETag:       *upRes.ETag,
				PartNumber: partNumber}
			return result, NoError
		}
	}
	return nil, NoError
}

func (ad *AwsAdapter) CompleteMultipartUpload(
	multipartUpload *pb.MultipartUpload,
	completeUpload *model.CompleteMultipartUpload,
	context context.Context) (*model.CompleteMultipartUploadResult, S3Error) {
	bucket := ad.backend.BucketName
	newObjectKey := multipartUpload.Bucket + "/" + multipartUpload.Key
	var completeParts []*awss3.CompletedPart
	for _, p := range completeUpload.Part {
		completePart := &awss3.CompletedPart{
			ETag:       aws.String(p.ETag),
			PartNumber: aws.Int64(p.PartNumber),
		}
		completeParts = append(completeParts, completePart)
	}
	completeInput := &awss3.CompleteMultipartUploadInput{
		Bucket:   &bucket,
		Key:      &newObjectKey,
		UploadId: &multipartUpload.UploadId,
		MultipartUpload: &awss3.CompletedMultipartUpload{
			Parts: completeParts,
		},
	}
	log.Logf("completeInput %v\n", completeInput)
	svc := awss3.New(ad.session)
	resp, err := svc.CompleteMultipartUpload(completeInput)
	if err != nil {
		log.Logf("completeMultipartUploadS3 failed, err:%v\n", err)
		return nil, S3Error{Code: 500, Description: err.Error()}
	}
	result := &model.CompleteMultipartUploadResult{
		Xmlns:    model.Xmlns,
		Location: *resp.Location,
		Bucket:   multipartUpload.Bucket,
		Key:      multipartUpload.Key,
		ETag:     *resp.ETag,
	}

	log.Logf("completeMultipartUploadS3 successfully, resp:%v\n", resp)
	return result, NoError
}

func (ad *AwsAdapter) AbortMultipartUpload(multipartUpload *pb.MultipartUpload, context context.Context) S3Error {
	bucket := ad.backend.BucketName
	newObjectKey := multipartUpload.Bucket + "/" + multipartUpload.Key
	abortInput := &awss3.AbortMultipartUploadInput{
		Bucket:   &bucket,
		Key:      &newObjectKey,
		UploadId: &multipartUpload.UploadId,
	}

	svc := awss3.New(ad.session)
	rsp, err := svc.AbortMultipartUpload(abortInput)
	if err != nil {
		log.Logf("abortMultipartUploadS3 failed, err:%v\n", err)
		return S3Error{Code: 500, Description: err.Error()}
	} else {
		log.Logf("abortMultipartUploadS3 successfully, rsp:%v\n", rsp)
	}
	return NoError
}

func (ad *AwsAdapter) ListParts(listParts *pb.ListParts, context context.Context) (*model.ListPartsOutput, S3Error) {
	return nil, NoError
}
