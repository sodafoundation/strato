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
	bk "github.com/opensds/multi-cloud/backend/pkg/db"
	backend "github.com/opensds/multi-cloud/backend/proto"
	bkpb "github.com/opensds/multi-cloud/backend/proto"
	"github.com/opensds/multi-cloud/block/pkg/db"
	pb "github.com/opensds/multi-cloud/block/proto"
	mockbkend "github.com/opensds/multi-cloud/testutils/backend/db/testing"
	"github.com/opensds/multi-cloud/testutils/block/collection"
	mockrepo "github.com/opensds/multi-cloud/testutils/block/db/testing"
	"testing"
	"time"
)


func TestGetVolume(t *testing.T) {
	var vol = pb.Volume{
		Id:                 "sample-Id",
		CreatedAt:          "sample-CreatedAt",
		UpdatedAt:          "sample-UpdatedAt",
		Name:               "sample-Name",
		Description:        "sample-Description",
		TenantId:           "sample-TenantId",
		UserId:             "sample-UserId",
		BackendId:          "sample-BackendId",
		Backend:            "sample-Backend",
		Size:               0,
		Type:               "sample-Type",
		Region:             "sample-Region",
		AvailabilityZone:   "sample-AvailabilityZone",
		Status:             "sample-Status",
		Iops:               0,
		SnapshotId:         "sample-snapshotID",
		Tags:               nil,

	}

	var volReq = &pb.GetVolumeRequest{
		Id:     "VolID",
		Volume: &vol,
	}

	var volResp = &pb.GetVolumeResponse{
		Volume: &vol,
	}

	var dbVol = &collection.SampleVolumes[0]

	ctx := context.Background()
	deadline := time.Now().Add(time.Duration(50) * time.Second)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	mockRepoClient := new(mockrepo.DBAdapter)
	mockRepoClient.On("GetVolume", ctx, "VolID").Return(dbVol, nil)
	db.DbAdapter = mockRepoClient

	testService := NewBlockService()
	err := testService.GetVolume(ctx, volReq, volResp)
	t.Log(err)
	mockRepoClient.AssertExpectations(t)

}


func TestListVolume(t *testing.T){

	var volReq = &pb.ListVolumeRequest{
		Limit:    1,
		Offset:   10,
		SortKeys: []string{"k1", "k2"},
		SortDirs: []string{"dir1", "dir2"},
		Filter:   map[string]string{"k1":"val1", "k2":"val2"},
	}

	var volResp = &pb.ListVolumeResponse{
		Volumes: collection.SampleListPBVolumes,
		Next:    2,
	}

	var dbVols = collection.SampleListModleVolumes

	ctx := context.Background()
	deadline := time.Now().Add(time.Duration(50) * time.Second)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	var filter = map[string]string{"k1":"val1", "k2":"val2"}
	mockRepoClient := new(mockrepo.DBAdapter)
	mockRepoClient.On("ListVolume", ctx, 1, 10, filter ).Return(dbVols, nil)
	db.DbAdapter = mockRepoClient

	testService := NewBlockService()
	err := testService.ListVolume(ctx, volReq, volResp)
	t.Log(err)
	mockRepoClient.AssertExpectations(t)

}


func TestDeleteVolume(t *testing.T) {
	var vol = pb.Volume{
		Id:                 "sample-Id",
		CreatedAt:          "sample-CreatedAt",
		UpdatedAt:          "sample-UpdatedAt",
		Name:               "sample-Name",
		Description:        "sample-Description",
		TenantId:           "sample-TenantId",
		UserId:             "sample-UserId",
		BackendId:          "sample-BackendId",
		Backend:            "sample-Backend",
		Size:               0,
		Type:               "sample-Type",
		Region:             "sample-Region",
		AvailabilityZone:   "sample-AvailabilityZone",
		Status:             "sample-Status",
		Iops:               0,
		SnapshotId:         "sample-snapshotID",
		Tags:               nil,

	}

	var volReq = &pb.DeleteVolumeRequest{
		Id:     "VolID",
		Volume: &vol,
	}

	var volResp = &pb.DeleteVolumeResponse{	}

	var dbVol = &collection.SampleVolumes[0]

    var fakeBackend = &backend.BackendDetail{
		Id:                   "sample-Id",
		TenantId:             "sample-TenantId",
		UserId:               "sample-UserId",
		Name:                 "sample-Name",
		Type:                 "sample-Type",
		Region:               "sample-Region",
		Endpoint:             "sample-Endpoint",
		BucketName:           "sample-BucketName",
		Access:               "sample-Access",
		Security:             "sample-Security",

	}
//======================Fake Backend==========================
//	var mockBackend = &bkenddata.SampleBackends[0]
//	var bkreq = &bkpb.GetBackendRequest{
//		Id: "Id",
//	}

	mockBackendDetail := bkpb.BackendDetail{
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

	var resp = &bkpb.GetBackendResponse{
		Backend: &mockBackendDetail,
	}

	ctx := context.Background()
	deadline := time.Now().Add(time.Duration(50) * time.Second)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	mockBKRepoClient := new(mockbkend.Repository)

	bk.Repo = mockBKRepoClient
//=====================================

	//fakeBackendClient := new(mockbackend.Repository)

	testService := NewBlockService()

	//backendClient := bakendmockrepo.BackendService{}

	mockRepoClient := new(mockrepo.DBAdapter)
	mockRepoClient.On("GetVolume", context.Background(), "VolID").Return( dbVol,nil)
	mockBKRepoClient.On("GetBackend", ctx, &backend.GetBackendRequest{Id:"Id"}).Return(resp, nil)
	//mockRepoClient.On("GetBackend", context.Background(), fakeBackendClient, dbVol.BackendId ).Return( fakeBackend,nil)
	mockRepoClient.On("DeleteVolume", context.Background(), "VolID").Return( nil)
	mockRepoClient.On("CreateStorageDriver", context.Background(), "VolID").Return( fakeBackend)
	db.DbAdapter = mockRepoClient

	err := testService.DeleteVolume(context.Background(), volReq, volResp)
	t.Log(err)
	mockRepoClient.AssertExpectations(t)


}



/*func TestCreateVolume(t *testing.T) {

	var vol = pb.Volume{
		Id:                 "sample-Id",
		CreatedAt:          "sample-CreatedAt",
		UpdatedAt:          "sample-UpdatedAt",
		Name:               "sample-Name",
		Description:        "sample-Description",
		TenantId:           "sample-TenantId",
		UserId:             "sample-UserId",
		BackendId:          "sample-BackendId",
		Backend:            "sample-Backend",
		Size:               0,
		Type:               "sample-Type",
		Region:             "sample-Region",
		AvailabilityZone:   "sample-AvailabilityZone",
		Status:             "sample-Status",
		Iops:               0,
		SnapshotId:         "sample-snapshotID",
		Tags:               nil,

	}

	var volReq = &pb.CreateVolumeRequest{
		Volume: &vol,
	}

	var volResp = &pb.CreateVolumeResponse{
		Volume: &vol,
	}

	var dbVol = &collection.SampleVolumes[0]

	var fakeBackend = &backend.BackendDetail{
		Id:                   "sample-Id",
		TenantId:             "sample-TenantId",
		UserId:               "sample-UserId",
		Name:                 "sample-Name",
		Type:                 "sample-Type",
		Region:               "sample-Region",
		Endpoint:             "sample-Endpoint",
		BucketName:           "sample-BucketName",
		Access:               "sample-Access",
		Security:             "sample-Security",

	}

	fakeBackendClient := new(mockbackend.Repository)

	testService := NewBlockService()

	mockRepoClient := new(mockrepo.DBAdapter)
	mockRepoClient.On("CreateVolume", context.Background(), volReq).Return( dbVol,nil)
	mockRepoClient.On("GetBackend", context.Background(), fakeBackendClient, dbVol.BackendId ).Return( fakeBackend,nil)
	mockRepoClient.On("DeleteVolume", context.Background(), "VolID").Return( nil)
	mockRepoClient.On("CreateStorageDriver", context.Background(), "VolID").Return( fakeBackend)
	db.DbAdapter = mockRepoClient

	err := testService.CreateVolume(context.Background(), volReq, volResp)
	t.Log(err)
	mockRepoClient.AssertExpectations(t)

}


func TestUpdateVolume(t *testing.T){
	var vol = pb.Volume{
		Id:                 "sample-Id",
		CreatedAt:          "sample-CreatedAt",
		UpdatedAt:          "sample-UpdatedAt",
		Name:               "sample-Name",
		Description:        "sample-Description",
		TenantId:           "sample-TenantId",
		UserId:             "sample-UserId",
		BackendId:          "sample-BackendId",
		Backend:            "sample-Backend",
		Size:               0,
		Type:               "sample-Type",
		Region:             "sample-Region",
		AvailabilityZone:   "sample-AvailabilityZone",
		Status:             "sample-Status",
		Iops:               0,
		SnapshotId:         "sample-snapshotID",
		Tags:               nil,

	}

	var volReq = &pb.UpdateVolumeRequest{
		Id: "volId",
		Volume: &vol,
	}

	var volResp = &pb.UpdateVolumeResponse{
		Volume: &vol,
	}

	var dbVol = &collection.SampleVolumes[0]

	var fakeBackend = &backend.BackendDetail{
		Id:                   "sample-Id",
		TenantId:             "sample-TenantId",
		UserId:               "sample-UserId",
		Name:                 "sample-Name",
		Type:                 "sample-Type",
		Region:               "sample-Region",
		Endpoint:             "sample-Endpoint",
		BucketName:           "sample-BucketName",
		Access:               "sample-Access",
		Security:             "sample-Security",

	}

	fakeBackendClient := new(mockbackend.Repository)

	testService := NewBlockService()

	mockRepoClient := new(mockrepo.DBAdapter)
	mockRepoClient.On("UpdateVolume", context.Background(), volReq).Return( dbVol,nil)
	mockRepoClient.On("GetBackend", context.Background(), fakeBackendClient, dbVol.BackendId ).Return( fakeBackend,nil)
	mockRepoClient.On("GetVolume", context.Background(), volReq.Id).Return( dbVol,nil)
	mockRepoClient.On("CreateStorageDriver", context.Background(), "VolID").Return( fakeBackend)
	db.DbAdapter = mockRepoClient

	err := testService.UpdateVolume(context.Background(), volReq, volResp)
	t.Log(err)
	mockRepoClient.AssertExpectations(t)
}
*/