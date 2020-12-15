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


package backend

import (
        log "github.com/sirupsen/logrus"
        "github.com/aws/aws-sdk-go/service/s3/s3manager"
        "github.com/aws/aws-sdk-go/aws/session"
	"bytes"
	"github.com/aws/aws-sdk-go/aws"
        "github.com/aws/aws-sdk-go/aws/credentials"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/opensds/multi-cloud/backend/proto"
)

func  awsCheck(backendDetail  *backend.BackendDetail)(error){
        Region := aws.String(backendDetail.Region)
        Endpoint:= aws.String(backendDetail.Endpoint)
        Credentials:= credentials.NewStaticCredentials(backendDetail.Access, backendDetail.Security, "")
        Body:= bytes.NewReader([]byte("large_multi_part_upload"))
        Bucket:= aws.String(backendDetail.BucketName)
        Key:= aws.String("EmptyBucket/")
        StorageClass:= aws.String("GLACIER")
        configuration:= &aws.Config{
                        Region: Region,
                        Endpoint: Endpoint,
                        Credentials: Credentials,
                        }

        sess,err := session.NewSession(configuration)

    if err != nil {
    log.Debug("Error in creating the session %v",err)
    return err 
    }

    uploader := s3manager.NewUploader(sess)
    input := &s3manager.UploadInput{
        Body:   Body,
        Bucket: Bucket,
        Key:    Key,
        StorageClass: StorageClass,
    }
    _, err = uploader.Upload(input)
    if err != nil {
        log.Debug("Received err in uploading object %v", err)
        return err
    }
log.Debug("put object[AWS S3] is successful\n")
//delete bucket
        svc := awss3.New(session.New(configuration))
        deleteInput := &awss3.DeleteObjectInput{
                        Bucket: Bucket,
                        Key: Key,
                  }

        _, err= svc.DeleteObject(deleteInput)
        if err!= nil {
                log.Debug("delete object[AWS S3] failed, objectId:%v, err:%v.\n", deleteInput, err)
                return err 
        }

        log.Debug("delete object[AWS S3] is successful\n")
	return nil

}

