// Copyright (c) 2018 Huawei Technologies Co., Ltd. All Rights Reserved.
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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/micro/go-log"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
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
		})

		if err != nil {
			log.Logf("Upload to aws failed:%v", err)
			return S3Error{Code: 500, Description: "Upload to aws failed"}
		}

	}

	return NoError
}

func (ad *AwsAdapter) GET(object *pb.Object, context context.Context) (io.ReadCloser, S3Error) {

	bucket := ad.backend.BucketName
	var buf []byte
	writer := aws.NewWriteAtBuffer(buf)
	newObjectKey := object.BucketName + "/" + object.ObjectKey
	if context.Value("operation") == "download" {
		downloader := s3manager.NewDownloader(ad.session)
		numBytes, err := downloader.DownloadWithContext(context, writer, &awss3.GetObjectInput{
			Bucket: &bucket,
			Key:    &newObjectKey,
		})
		if err != nil {
			log.Logf("Download failed:%v", err)
			return nil, S3Error{Code: 500, Description: "Download failed"}
		} else {
			log.Logf("Download succeed, bytes:%d\n", numBytes)
			body := bytes.NewReader(writer.Bytes())
			reader := ioutil.NopCloser(body)

			return reader, NoError
		}

	}

	return nil, NoError
}

func (ad *AwsAdapter) DELETE(object *pb.DeleteObjectInput, ctx context.Context) S3Error {

	bucket := ad.backend.BucketName

	newObjectKey := object.Bucket + "/" + object.Key

	svc := awss3.New(ad.session)
	deleteInput := awss3.DeleteObjectInput{Bucket: &bucket, Key: &newObjectKey}

	_, err := svc.DeleteObject(&deleteInput)
	if err != nil {
		log.Logf("Delete object failed, err:%v\n", err)
		return InternalError
	}

	log.Logf("Delete object %s from aws successfully.\n", newObjectKey)

	return NoError
}
