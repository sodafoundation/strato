// Copyright 2019 The SODA Authors.
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
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/soda/multi-cloud/backend/pkg/db"
	"github.com/soda/multi-cloud/backend/pkg/model"
	"github.com/soda/multi-cloud/backend/pkg/utils/constants"
	pb "github.com/soda/multi-cloud/backend/proto"
	"github.com/soda/multi-cloud/testutils/backend/collection"
	mockrepo "github.com/soda/multi-cloud/testutils/backend/db/testing"
)

type MockBackendService struct {
	mock.Mock
}

//====================Mock methods of type (_m *MockBackendService)=============================

func (_m *MockBackendService) CreateBackend(ctx context.Context, in *pb.CreateBackendRequest, out *pb.CreateBackendResponse) error {
	args := _m.Called()
	result := args.Get(0)

	return result.(error)
}

func (_m *MockBackendService) GetBackend(ctx context.Context, in *pb.GetBackendRequest, out *pb.GetBackendResponse) error {
	args := _m.Called()
	result := args.Get(0)

	return result.(error)
}

func (_m *MockBackendService) ListBackend(ctx context.Context, in *pb.ListBackendRequest, out *pb.ListBackendResponse) error {
	args := _m.Called()
	result := args.Get(0)

	return result.(error)
}

func (_m *MockBackendService) UpdateBackend(ctx context.Context, in *pb.UpdateBackendRequest, out *pb.UpdateBackendResponse) error {
	args := _m.Called()
	result := args.Get(0)

	return result.(error)
}

func (_m *MockBackendService) DeleteBackend(ctx context.Context, in *pb.DeleteBackendRequest, out *pb.DeleteBackendResponse) error {
	args := _m.Called()
	result := args.Get(0)

	return result.(error)
}

func (_m *MockBackendService) ListType(ctx context.Context, in *pb.ListTypeRequest, out *pb.ListTypeResponse) error {
	args := _m.Called()
	result := args.Get(0)

	return result.(error)
}

func (_m *MockBackendService) CreateTier(ctx context.Context, in *pb.CreateTierRequest, out *pb.CreateTierResponse) error {
	args := _m.Called()
	result := args.Get(0)

	return result.(error)
}

func (_m *MockBackendService) GetTier(ctx context.Context, in *pb.GetTierRequest, out *pb.GetTierResponse) error {
	args := _m.Called()
	result := args.Get(0)

	return result.(error)
}

func (_m *MockBackendService) ListTiers(ctx context.Context, in *pb.ListTierRequest, out *pb.ListTierResponse) error {
	args := _m.Called()
	result := args.Get(0)

	return result.(error)
}

func (_m *MockBackendService) UpdateTier(ctx context.Context, in *pb.UpdateTierRequest, out *pb.UpdateTierResponse) error {
	args := _m.Called()
	result := args.Get(0)

	return result.(error)
}

func (_m *MockBackendService) DeleteTier(ctx context.Context, in *pb.DeleteTierRequest, out *pb.DeleteTierResponse) error {
	args := _m.Called()
	result := args.Get(0)

	return result.(error)
}

//===============================Test methods Test<MethodName> ==================================

func TestGetBackend(t *testing.T) {
	var mockBackend = &collection.SampleBackends[0]
	var req = &pb.GetBackendRequest{
		Id: "Id",
	}

	mockBackendDetail := pb.BackendDetail{
		Id:         "3769855c-b103-11e7-b772-17b880d2f537",
		TenantId:   "backend-tenant",
		UserId:     "backend-userID",
		Name:       "backend-name",
		Type:       "backend-type",
		Region:     "backend-region",
		Endpoint:   "backend-endpoint",
		BucketName: "backend-bucket",
		Access:     "backend-access",
		Security:   "backend-security",
	}

	var resp = &pb.GetBackendResponse{
		Backend: &mockBackendDetail,
	}

	ctx := context.Background()
	deadline := time.Now().Add(time.Duration(50) * time.Second)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	mockRepoClient := new(mockrepo.Repository)
	mockRepoClient.On("GetBackend", ctx, "Id").Return(mockBackend, nil)
	db.Repo = mockRepoClient

	testService := NewBackendService()
	err := testService.GetBackend(ctx, req, resp)
	t.Log(err)
	mockRepoClient.AssertExpectations(t)
}

func TestListBackend(t *testing.T) {
	//var mockBackend = &collection.SampleBackends[0]
	var req = &pb.ListBackendRequest{
		Limit:    10,
		Offset:   20,
		SortKeys: []string{"k1", "k2"},
		SortDirs: []string{"dir1", "dir2"},
		Filter:   map[string]string{"k1": "val1"},
	}

	var pbBackend = []*model.Backend{
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

	var pbBackendDetail = []*pb.BackendDetail{
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

	var resp = &pb.ListBackendResponse{
		Backends: pbBackendDetail,
	}

	ctx := context.Background()
	deadline := time.Now().Add(time.Duration(50) * time.Second)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	mockRepoClient := new(mockrepo.Repository)
	mockRepoClient.On("ListBackend", ctx, 10, 20, map[string]string{"k1": "val1"}).Return(pbBackend, nil)
	db.Repo = mockRepoClient

	testService := NewBackendService()
	err := testService.ListBackend(ctx, req, resp)
	t.Log(err)
	mockRepoClient.AssertExpectations(t)
}

func TestCreateBackend(t *testing.T) {
	var mockBackend = &collection.SampleCreateBackend[0]

	mockBackendDetail := pb.BackendDetail{
		Id:         "",
		TenantId:   "sample-backend-tenantID",
		UserId:     "sample-backend-userID",
		Name:       "sample-backend-name",
		Type:       "sample-backend-type",
		Region:     "sample-backend-region",
		Endpoint:   "sample-backend-endpoint",
		BucketName: "sample-backend-bucketname",
		Access:     "sample-backend-access",
		Security:   "sample-backend-security",
	}

	var req = &pb.CreateBackendRequest{
		Backend: &mockBackendDetail,
	}
	var resp = &pb.CreateBackendResponse{
		Backend: &mockBackendDetail,
	}

	ctx := context.Background()
	deadline := time.Now().Add(time.Duration(50) * time.Second)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	mockRepoClient := new(mockrepo.Repository)
	mockRepoClient.On("CreateBackend", ctx, mockBackend).Return(mockBackend, nil)
	db.Repo = mockRepoClient

	testService := NewBackendService()
	err := testService.CreateBackend(ctx, req, resp)
	t.Log(err)
	mockRepoClient.AssertExpectations(t)
}

func TestDeleteBackend(t *testing.T) {
	var req = &pb.DeleteBackendRequest{
		Id: "Id",
	}

	var resp = &pb.DeleteBackendResponse{}

	ctx := context.Background()
	deadline := time.Now().Add(time.Duration(50) * time.Second)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	mockRepoClient := new(mockrepo.Repository)
	mockRepoClient.On("DeleteBackend", ctx, "Id").Return(nil)
	db.Repo = mockRepoClient

	testService := NewBackendService()
	err := testService.DeleteBackend(ctx, req, resp)
	t.Log(err)
	mockRepoClient.AssertExpectations(t)
}

func TestUpdateBackend(t *testing.T) {
	var mockBackend = &collection.SampleBackends[0]
	var req = &pb.UpdateBackendRequest{
		Id:       "Id",
		Access:   "access",
		Security: "security",
	}

	mockBackendDetail := pb.BackendDetail{
		Id:         "3769855c-b103-11e7-b772-17b880d2f537",
		TenantId:   "backend-tenant",
		UserId:     "backend-userID",
		Name:       "backend-name",
		Type:       "backend-type",
		Region:     "backend-region",
		Endpoint:   "backend-endpoint",
		BucketName: "backend-bucket",
		Access:     "backend-access",
		Security:   "backend-security",
	}

	var resp = &pb.UpdateBackendResponse{
		Backend: &mockBackendDetail,
	}

	ctx := context.Background()
	deadline := time.Now().Add(time.Duration(50) * time.Second)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	mockRepoClient := new(mockrepo.Repository)
	mockRepoClient.On("GetBackend", ctx, "Id").Return(mockBackend, nil)
	mockRepoClient.On("UpdateBackend", ctx, mockBackend).Return(mockBackend, nil)
	db.Repo = mockRepoClient

	testService := NewBackendService()
	err := testService.UpdateBackend(ctx, req, resp)
	t.Log(err)
	mockRepoClient.AssertExpectations(t)
}

func TestListTypes(t *testing.T) {
	var req = &pb.ListTypeRequest{
		Limit:    1,
		Offset:   2,
		SortKeys: []string{"key1", "key2"},
		SortDirs: []string{"dir1", "dir2"},
		Filter:   map[string]string{"key1": "val1", "key2": "val2"},
	}

	tmpDetail1 := pb.TypeDetail{
		Name:        constants.BackendTypeAws,
		Description: "AWS Simple Cloud Storage Service(S3)",
	}
	tmpDetail2 := pb.TypeDetail{
		Name:        constants.BackendTypeObs,
		Description: "Huawei Object Storage Service(OBS)",
	}

	typeList := []*pb.TypeDetail{
		&tmpDetail1,
		&tmpDetail2,
	}

	var resp = &pb.ListTypeResponse{
		Types: typeList,
		Next:  99,
	}

	ctx := context.Background()
	deadline := time.Now().Add(time.Duration(50) * time.Second)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	testService := NewBackendService()
	err := testService.ListType(ctx, req, resp)
	t.Log(err)
}

func TestCreateTier(t *testing.T) {
	var mockTier = &collection.SampleCreateTier[0]

	mockTierDetail := pb.Tier{
		Id:       "",
		TenantId: "sample-tier-tenantID",
		Name:     "sample-tier-name",
		Backends: []string{"sample-tier-backend-1", "sample-tier-backend-2"},
	}

	var req = &pb.CreateTierRequest{
		Tier: &mockTierDetail,
	}
	var resp = &pb.CreateTierResponse{
		Tier: &mockTierDetail,
	}

	ctx := context.Background()
	deadline := time.Now().Add(time.Duration(50) * time.Second)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	mockRepoClient := new(mockrepo.Repository)
	mockRepoClient.On("CreateTier", ctx, mockTier).Return(mockTier, nil)
	db.Repo = mockRepoClient

	testService := NewBackendService()
	err := testService.CreateTier(ctx, req, resp)
	t.Log(err)
	mockRepoClient.AssertExpectations(t)

}

func TestGetTier(t *testing.T) {
	var mockTier = &collection.SampleTiers[0]
	var req = &pb.GetTierRequest{
		Id: "Id",
	}

	mockTierDetail := pb.Tier{
		Id:       "3769855c-b103-11e7-b772-17b880d2f537",
		TenantId: "sample-tiers-tenantID",
		Name:     "sample-tiers-name",
		Backends: []string{"sample-tier-backend-1", "sample-tier-backend-2"},
	}

	var resp = &pb.GetTierResponse{
		Tier: &mockTierDetail,
	}

	ctx := context.Background()
	deadline := time.Now().Add(time.Duration(50) * time.Second)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	mockRepoClient := new(mockrepo.Repository)
	mockRepoClient.On("GetTier", ctx, "Id").Return(mockTier, nil)
	db.Repo = mockRepoClient

	testService := NewBackendService()
	err := testService.GetTier(ctx, req, resp)
	t.Log(err)
	mockRepoClient.AssertExpectations(t)
}

func TestListTiers(t *testing.T) {
	var req = &pb.ListTierRequest{
		Limit:    10,
		Offset:   20,
		SortKeys: []string{"k1", "k2"},
		SortDirs: []string{"dir1", "dir2"},
	}

	var pbTier = []*model.Tier{
		{Id: "tierId",
			TenantId: "tenantId",
			Name:     "name",
			Backends: []string{"backends"},
		},
	}

	var pbTierDetail = []*pb.Tier{
		{Id: "tierId",
			TenantId: "tenantId",
			Name:     "name",
			Backends: []string{"backends"},
		},
	}

	var resp = &pb.ListTierResponse{
		Tiers: pbTierDetail,
	}

	ctx := context.Background()
	deadline := time.Now().Add(time.Duration(50) * time.Second)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	mockRepoClient := new(mockrepo.Repository)
	mockRepoClient.On("ListTiers", ctx, 10, 20).Return(pbTier, nil)
	db.Repo = mockRepoClient

	testService := NewBackendService()
	err := testService.ListTiers(ctx, req, resp)
	t.Log(err)
	mockRepoClient.AssertExpectations(t)
}

func TestUpdateTier(t *testing.T) {

	var mockTier = &collection.SampleUpdateTier[0]
	mockTierReqDetail := pb.Tier{
		Id:       "Id",
		TenantId: "tier-tenant",
		Name:     "tier-name",
		Backends: []string{"Backends"},
	}

	var req = &pb.UpdateTierRequest{
		Tier: &mockTierReqDetail,
	}

	mockTierResDetail := pb.Tier{
		Id:       "Id",
		TenantId: "tier-tenant",
		Name:     "ter-name",
		Backends: []string{"Backends"},
	}

	var resp = &pb.UpdateTierResponse{
		Tier: &mockTierResDetail,
	}

	ctx := context.Background()
	deadline := time.Now().Add(time.Duration(50) * time.Second)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	mockRepoClient := new(mockrepo.Repository)
	mockRepoClient.On("GetTier", ctx, "Id").Return(mockTier, nil)
	mockRepoClient.On("UpdateTier", ctx, mockTier).Return(mockTier, nil)
	db.Repo = mockRepoClient

	testService := NewBackendService()
	err := testService.UpdateTier(ctx, req, resp)
	t.Log(err)
	mockRepoClient.AssertExpectations(t)

}

func TestDeleteTier(t *testing.T) {
	var req = &pb.DeleteTierRequest{
		Id: "Id",
	}
	var resp = &pb.DeleteTierResponse{}

	ctx := context.Background()
	deadline := time.Now().Add(time.Duration(50) * time.Second)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	mockRepoClient := new(mockrepo.Repository)
	mockRepoClient.On("DeleteTier", ctx, "Id").Return(nil)
	db.Repo = mockRepoClient

	testService := NewBackendService()
	err := testService.DeleteTier(ctx, req, resp)
	t.Log(err)
	mockRepoClient.AssertExpectations(t)
}
