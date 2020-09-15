// Copyright 2020 The SODA Authors.
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
	"github.com/micro/go-micro/v2/client"
	bkendpb "github.com/opensds/multi-cloud/backend/proto"
	"github.com/opensds/multi-cloud/file/pkg/db"
	pb "github.com/opensds/multi-cloud/file/proto"
	bksvc "github.com/opensds/multi-cloud/testutils/backend/proto"
	sdtest "github.com/opensds/multi-cloud/testutils/contrib/datastore/drivers"
	"github.com/opensds/multi-cloud/testutils/file/collection"
	dbtest "github.com/opensds/multi-cloud/testutils/file/db/testing"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
)

//====================Mock methods of type (_m *DBAdapter)=============================
type MockFileService struct {
	mock.Mock
}

func (_m *MockFileService) GetFileShare(ctx context.Context, in *pb.GetFileShareRequest, out *pb.GetFileShareResponse) error {
	args := _m.Called()
	result := args.Get(0)

	return result.(error)
}

func (_m *MockFileService) ListFileShare(ctx context.Context, in *pb.ListFileShareRequest, out *pb.ListFileShareResponse) error {
	args := _m.Called()
	result := args.Get(0)

	return result.(error)
}

func (_m *MockFileService) CreateFileShare(ctx context.Context, in *pb.CreateFileShareRequest, out *pb.CreateFileShareResponse) error {
	args := _m.Called()
	result := args.Get(0)

	return result.(error)
}

func (_m *MockFileService) DeleteFileShare(ctx context.Context, in *pb.DeleteFileShareRequest, out *pb.DeleteFileShareResponse) error {
	args := _m.Called()
	result := args.Get(0)

	return result.(error)
}

func (_m *MockFileService) SyncFileShare(ctx context.Context, fs *pb.FileShare, backend *bkendpb.BackendDetail) {
	return
}

func (_m *MockFileService) UpdateFileShare(ctx context.Context, in *pb.UpdateFileShareRequest, out *pb.UpdateFileShareResponse) error {
	args := _m.Called()
	result := args.Get(0)

	return result.(error)
}

type MockBackendService struct {
	mock.Mock
}

func (_m *MockBackendService) GetBackend(ctx context.Context, in *bkendpb.GetBackendRequest, opts ...client.CallOption) (*bkendpb.GetBackendResponse, error) {
	args := _m.Called()
	result := args.Get(0)

	return result.(*bkendpb.GetBackendResponse), args.Error(1)
}

//====================Test<method> of type (_m *DBAdapter)=============================

func TestListFileShareHappy(t *testing.T) {
	var fileshares = collection.SampleListFileShares

	var pbFileshares = []*pb.FileShare{
		{
			Id:                 "3769855c-a102-11e7-b772-17b880d2f537",
			CreatedAt:          "CreatedAt",
			UpdatedAt:          "UpdatedAt",
			Name:               "sample-fileshare-01",
			Description:        "This is first sample fileshare for testing",
			UserId:             "UserID",
			Backend:            "Backend",
			BackendId:          "BackendId",
			Size:               int64(2000),
			Type:               "Type",
			TenantId:           "TenantId",
			Status:             "available",
			Region:             "asia",
			AvailabilityZone:   "default",
			Protocols:          []string{"iscsi"},
			SnapshotId:         "snapshotid",
			Encrypted:          false,
			EncryptionSettings: map[string]string{"foo": "bar"},
		},
	}
	var req = &pb.ListFileShareRequest{
		Limit:    100,
		Offset:   200,
		SortKeys: []string{"k1", "k2"},
		SortDirs: []string{"dir1", "dir2"},
		Filter:   map[string]string{"foo": "bar"},
	}

	var resp = &pb.ListFileShareResponse{
		Fileshares: pbFileshares,
		Next:       99,
	}

	ctx := context.Background()
	deadline := time.Now().Add(time.Duration(50) * time.Second)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	mockDBClient := new(dbtest.DBAdapter)
	mockDBClient.On("ListFileShare", ctx, 100, 200, map[string]string{"foo": "bar"}).Return(fileshares, nil)
	db.DbAdapter = mockDBClient

	testService := NewFileService()
	err := testService.ListFileShare(ctx, req, resp)
	t.Log(err)
	mockDBClient.AssertExpectations(t)
}

func TestGetFileShare(t *testing.T) {
	var fileshares = &collection.SampleGetFileShares[0]
	var pbFileshares = []*pb.FileShare{
		{
			Id:               "3769855c-a102-11e7-b772-17b880d2f537",
			CreatedAt:        "CreatedAt",
			UpdatedAt:        "UpdatedAt",
			Name:             "sample-fileshare-01",
			Description:      "This is first sample fileshare for testing",
			TenantId:         "TenantId",
			Status:           "available",
			Region:           "asia",
			AvailabilityZone: "default",
		},
	}

	var req = &pb.GetFileShareRequest{
		Id:        "sample-fileshare-001",
		Fileshare: pbFileshares[0],
	}

	var resp = &pb.GetFileShareResponse{
		Fileshare: pbFileshares[0],
	}

	ctx := context.Background()
	deadline := time.Now().Add(time.Duration(50) * time.Second)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	mockDBClient := new(dbtest.DBAdapter)
	mockDBClient.On("GetFileShare", ctx, "sample-fileshare-001").Return(fileshares, nil)
	db.DbAdapter = mockDBClient

	testService := NewFileService()
	err := testService.GetFileShare(ctx, req, resp)
	t.Log(err)
	mockDBClient.AssertExpectations(t)

}

func TestCreateFileShare(t *testing.T) {

	var pbFileshares = []*pb.FileShare{
		{
			Id:                 "3769855c-a102-11e7-b772-17b880d2f530",
			CreatedAt:          "CreatedAt",
			UpdatedAt:          "UpdatedAt",
			Name:               "sample-fileshare-01",
			Description:        "This is first sample fileshare for testing",
			UserId:             "UserID",
			Backend:            "Backend",
			BackendId:          "BackendId",
			Size:               int64(2000),
			Type:               "Type",
			TenantId:           "TenantId",
			Status:             "available",
			Region:             "asia",
			AvailabilityZone:   "default",
			Protocols:          []string{"iscsi"},
			SnapshotId:         "snapshotid",
			Encrypted:          false,
			EncryptionSettings: map[string]string{"foo": "bar"},
		},
	}

	var pbBackendDetail = []*bkendpb.BackendDetail{
		{Id: "backendId",
			TenantId:   "tenantId",
			UserId:     "userId",
			Name:       "name",
			Type:       "type",
			Region:     "region",
			Endpoint:   "endpoint",
			BucketName: "bucketName",
			Access:     "access",
			Security:   "security",
		},
	}
	var req = &pb.CreateFileShareRequest{
		Fileshare: pbFileshares[0],
	}

	var resp = &pb.CreateFileShareResponse{
		Fileshare: pbFileshares[0],
	}

	var backendResp = &bkendpb.GetBackendResponse{
		Backend: pbBackendDetail[0],
	}

	ctx := context.Background()
	deadline := time.Now().Add(time.Duration(50) * time.Second)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	mockDBClient := new(dbtest.DBAdapter)
	mockStorageDriver := new(sdtest.DriverFactory)
	mockBackendClient := new(bksvc.BackendService)

	mockStorageDriver.On("CreateStorageDriver", &pbBackendDetail).Return(pbBackendDetail)
	mockBackendClient.On("GetBackend", ctx, "backendID").Return(backendResp, nil)
	db.DbAdapter = mockDBClient

	testService := NewFileService()
	err := testService.CreateFileShare(ctx, req, resp)

	t.Log(err)
	mockDBClient.AssertExpectations(t)

}

func TestDeleteFileShare(t *testing.T) {
	var fileshares = &collection.SampleGetFileShares[0]
	var pbFileshares = []*pb.FileShare{
		{
			Id:               "3769855c-a102-11e7-b772-17b880d2f537",
			CreatedAt:        "CreatedAt",
			UpdatedAt:        "UpdatedAt",
			Name:             "sample-fileshare-01",
			Description:      "This is first sample fileshare for testing",
			TenantId:         "TenantId",
			Status:           "available",
			Region:           "asia",
			AvailabilityZone: "default",
		},
	}

	var req = &pb.DeleteFileShareRequest{
		Id:        "sample-fileshare-001",
		Fileshare: pbFileshares[0],
	}

	var resp = &pb.DeleteFileShareResponse{

	}

	ctx := context.Background()
	deadline := time.Now().Add(time.Duration(50) * time.Second)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	mockDBClient := new(dbtest.DBAdapter)
	mockDBClient.On("GetFileShare", ctx, "sample-fileshare-001").Return(fileshares, nil)
	db.DbAdapter = mockDBClient

	testService := NewFileService()
	err := testService.DeleteFileShare(ctx, req, resp)
	t.Log(err)
	mockDBClient.AssertExpectations(t)

}

func TestUpdateFileShare(t *testing.T) {
	var fileshares = &collection.SampleGetFileShares[0]
	var pbFileshares = []*pb.FileShare{
		{
			Id:               "3769855c-a102-11e7-b772-17b880d2f537",
			CreatedAt:        "CreatedAt",
			UpdatedAt:        "UpdatedAt",
			Name:             "sample-fileshare-01",
			Description:      "This is first sample fileshare for testing",
			TenantId:         "TenantId",
			Status:           "available",
			Region:           "asia",
			AvailabilityZone: "default",
		},
	}

	var req = &pb.UpdateFileShareRequest{
		Id:        "sample-fileshare-001",
		Fileshare: pbFileshares[0],
	}

	var resp = &pb.UpdateFileShareResponse{
		Fileshare: pbFileshares[0],
	}

	ctx := context.Background()
	deadline := time.Now().Add(time.Duration(50) * time.Second)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	mockDBClient := new(dbtest.DBAdapter)
	mockDBClient.On("GetFileShare", ctx, "sample-fileshare-001").Return(fileshares, nil)
	db.DbAdapter = mockDBClient

	testService := NewFileService()
	err := testService.UpdateFileShare(ctx, req, resp)
	t.Log(err)
	mockDBClient.AssertExpectations(t)

}
