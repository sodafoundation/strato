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

package aliyun

import (
	"context"
	"io"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/micro/go-log"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	pb "github.com/opensds/multi-cloud/s3/proto"
)

type AliyunAdapter struct {
	backend *backendpb.BackendDetail
	client  *oss.Client
}

func Init(backend *backendpb.BackendDetail) *AliyunAdapter {
	endpoint := backend.Endpoint
	AccessKeyId := backend.Access
	AccessKeySecret := backend.Security

	client, err := oss.New(endpoint, AccessKeyId, AccessKeySecret)
	if err != nil {
		log.Logf("Access aliyun failed:%v", err)
		return nil
	}

	adap := &AliyunAdapter{backend: backend, client: client}
	return adap
}

func (ad *AliyunAdapter) PUT(stream io.Reader, object *pb.Object, ctx context.Context) S3Error {

	bucketName := ad.backend.BucketName

	bucket, err := ad.client.Bucket(bucketName)
	if err != nil {
		log.Logf("Access bucket failed:%v", err)
		return S3Error{Code: 500, Description: "Access bucket failed"}
	}

	if ctx.Value("operation") == "upload" {
		newObjectKey := object.BucketName + "/" + object.ObjectKey

		err = bucket.PutObject(newObjectKey, stream)

		if err != nil {
			log.Logf("Upload to aliyun failed:%v", err)
			return S3Error{Code: 500, Description: "Upload to aliyun failed"}
		}
	}

	return NoError
}

func (ad *AliyunAdapter) DELETE(object *pb.DeleteObjectInput, ctx context.Context) S3Error {

	newObjectKey := object.Bucket + "/" + object.Key
	bucketName := ad.backend.BucketName

	bucket, err := ad.client.Bucket(bucketName)
	if err != nil {
		log.Logf("Access bucket failed:%v", err)
		return S3Error{Code: 500, Description: "Access bucket failed"}
	}

	deleteErr := bucket.DeleteObject(newObjectKey)
	if deleteErr != nil {
		log.Logf("Delete object failed:%v", err)
		return S3Error{Code: 500, Description: "Delete object failed"}
	}

	log.Logf("Delete object %s from aliyun successfully.\n", newObjectKey)
	return NoError
}
