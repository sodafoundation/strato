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

package datastore

import (
	"context"
	"io"

	"github.com/opensds/multi-cloud/api/pkg/s3/datastore/aws"
	"github.com/opensds/multi-cloud/api/pkg/s3/datastore/azure"
	"github.com/opensds/multi-cloud/api/pkg/s3/datastore/hws"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	pb "github.com/opensds/multi-cloud/s3/proto"
)

// Init function can perform some initialization work of different datastore.
func Init(backend *backendpb.BackendDetail) (DataStoreAdapter,S3Error) {
	var StoreAdapter DataStoreAdapter

	switch backend.Type {
	case "azure-blob":
		//DbAdapter = mongo.Init(strings.Split(db.Endpoint, ","))
		StoreAdapter = azure.Init(backend) 
		return StoreAdapter,NoError
	case "hw-obs":
		//DbAdapter = mongo.Init(strings.Split(db.Endpoint, ","))
		StoreAdapter = hws.Init(backend)
		return StoreAdapter,NoError
	case "aws-s3":
		//DbAdapter = mongo.Init(strings.Split(db.Endpoint, ","))
		StoreAdapter = aws.Init(backend)
		return StoreAdapter,NoError
	default:

	}
	return nil,NoSuchType
}

func Exit(backendType string) {

}

type DataStoreAdapter interface {
	PUT(stream io.Reader, object *pb.Object, context context.Context) S3Error
	GET(object *pb.Object, context context.Context, start int64, end int64) (io.ReadCloser, S3Error)
	DELETE(object *pb.DeleteObjectInput, context context.Context) S3Error

	GetObjectInfo(bucketName string, key string, context context.Context) (*pb.Object, S3Error)
	InitMultipartUpload(object *pb.Object, context context.Context) (*pb.MultipartUpload, S3Error)
	UploadPart(stream io.Reader, multipartUpload *pb.MultipartUpload, partNumber int64, upBytes int64, context context.Context) (*model.UploadPartResult, S3Error)
	CompleteMultipartUpload(multipartUpload *pb.MultipartUpload, completeUpload *model.CompleteMultipartUpload, context context.Context) (*model.CompleteMultipartUploadResult, S3Error)
	AbortMultipartUpload(multipartUpload *pb.MultipartUpload, context context.Context) S3Error

	ListParts(listParts *pb.ListParts, context context.Context) (*model.ListPartsOutput, S3Error)
}
