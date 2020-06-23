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

package s3

import (
	"context"
	"github.com/micro/go-micro/client"
	"testing"

	"github.com/emicklei/go-restful"
	backend "github.com/opensds/multi-cloud/backend/proto"
	//. "github.com/opensds/multi-cloud/s3/pkg/exception"
	s3 "github.com/opensds/multi-cloud/s3/proto"
)

func TestAPIService_BucketPut(t *testing.T) {
	type fields struct {
		s3Client      s3.S3Service
		backendClient backend.BackendService
	}
	type args struct {
		request  *restful.Request
		response *restful.Response
	}

	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{name:"test1", fields:fields{s3Client:new_fake_s3_interface(),backendClient:new_fake_bucket_BackendService()},args:args{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &APIService{
				s3Client:      tt.fields.s3Client,
				backendClient: tt.fields.backendClient,
			}
			s.BucketPut(tt.args.request, tt.args.response)
		})
	}
}

type fake_buckendService struct {}

func (fake_buckendService) CreateBackend(ctx context.Context, in *backend.CreateBackendRequest, opts ...client.CallOption) (*backend.CreateBackendResponse, error) {
	return nil, nil
}

func (fake_buckendService) GetBackend(ctx context.Context, in *backend.GetBackendRequest, opts ...client.CallOption) (*backend.GetBackendResponse, error) {
	return nil, nil
}

func (fake_buckendService) ListBackend(ctx context.Context, in *backend.ListBackendRequest, opts ...client.CallOption) (*backend.ListBackendResponse, error) {
	return nil, nil
}

func (fake_buckendService) UpdateBackend(ctx context.Context, in *backend.UpdateBackendRequest, opts ...client.CallOption) (*backend.UpdateBackendResponse, error) {
	return nil, nil
}

func (fake_buckendService) DeleteBackend(ctx context.Context, in *backend.DeleteBackendRequest, opts ...client.CallOption) (*backend.DeleteBackendResponse, error) {
	return nil, nil
}

func (fake_buckendService) ListType(ctx context.Context, in *backend.ListTypeRequest, opts ...client.CallOption) (*backend.ListTypeResponse, error) {
	return nil, nil
}

func new_fake_bucket_BackendService() backend.BackendService {
	return &fake_buckendService{}
}

type fake_s3Service struct{}

func (fake_s3Service) ListBuckets(ctx context.Context, in *s3.BaseRequest, opts ...client.CallOption) (*s3.ListBucketsResponse, error) {
	return nil, nil
}

func (fake_s3Service) CreateBucket(ctx context.Context, in *s3.Bucket, opts ...client.CallOption) (*s3.BaseResponse, error) {
	resp :=&s3.BaseResponse{}
	resp.ErrorCode="400"
	return resp, nil
}

func (fake_s3Service) DeleteBucket(ctx context.Context, in *s3.Bucket, opts ...client.CallOption) (*s3.BaseResponse, error) {
	return nil, nil
}

func (fake_s3Service) GetBucket(ctx context.Context, in *s3.Bucket, opts ...client.CallOption) (*s3.Bucket, error) {
	return nil, nil
}

func (fake_s3Service) ListObjects(ctx context.Context, in *s3.ListObjectsRequest, opts ...client.CallOption) (*s3.ListObjectResponse, error) {
	return nil, nil
}

func (fake_s3Service) CountObjects(ctx context.Context, in *s3.ListObjectsRequest, opts ...client.CallOption) (*s3.CountObjectsResponse, error) {
	return nil, nil
}

func (fake_s3Service) CreateObject(ctx context.Context, in *s3.Object, opts ...client.CallOption) (*s3.BaseResponse, error) {
	return nil, nil
}

func (fake_s3Service) UpdateObject(ctx context.Context, in *s3.Object, opts ...client.CallOption) (*s3.BaseResponse, error) {
	return nil, nil
}

func (fake_s3Service) GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...client.CallOption) (*s3.Object, error) {
	return nil, nil
}

func (fake_s3Service) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, opts ...client.CallOption) (*s3.BaseResponse, error) {
	return nil, nil
}

func (fake_s3Service) GetTierMap(ctx context.Context, in *s3.BaseRequest, opts ...client.CallOption) (*s3.GetTierMapResponse, error) {
	return nil, nil
}

func (fake_s3Service) UpdateObjMeta(ctx context.Context, in *s3.UpdateObjMetaRequest, opts ...client.CallOption) (*s3.BaseResponse, error) {
	return nil, nil
}

func (fake_s3Service) GetStorageClasses(ctx context.Context, in *s3.BaseRequest, opts ...client.CallOption) (*s3.GetStorageClassesResponse, error) {
	return nil, nil
}

func (fake_s3Service) GetBackendTypeByTier(ctx context.Context, in *s3.GetBackendTypeByTierRequest, opts ...client.CallOption) (*s3.GetBackendTypeByTierResponse, error) {
	return nil, nil
}

func (fake_s3Service) DeleteBucketLifecycle(ctx context.Context, in *s3.DeleteLifecycleInput, opts ...client.CallOption) (*s3.BaseResponse, error) {
	return nil, nil
}

func (fake_s3Service) UpdateBucket(ctx context.Context, in *s3.Bucket, opts ...client.CallOption) (*s3.BaseResponse, error) {
	return nil, nil
}

func (fake_s3Service) AddUploadRecord(ctx context.Context, in *s3.MultipartUploadRecord, opts ...client.CallOption) (*s3.BaseResponse, error) {
	return nil, nil
}

func (fake_s3Service) DeleteUploadRecord(ctx context.Context, in *s3.MultipartUploadRecord, opts ...client.CallOption) (*s3.BaseResponse, error) {
	return nil, nil
}

func (fake_s3Service) ListUploadRecord(ctx context.Context, in *s3.ListMultipartUploadRequest, opts ...client.CallOption) (*s3.ListMultipartUploadResponse, error) {
	return nil, nil
}

func new_fake_s3_interface() s3.S3Service {
	return &fake_s3Service{}
}
