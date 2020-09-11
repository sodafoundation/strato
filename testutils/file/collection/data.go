// Copyright 2017 The OpenSDS Authors.
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

/*

This package includes a collection of fake stuffs for testing work.
*/

package collection

import (
	backendModel "github.com/opensds/multi-cloud/backend/pkg/model"
	fileModel "github.com/opensds/multi-cloud/file/pkg/model"
)

//==============================Mock FileShare Data===================================
var (
	size        = int64(2000)
	sizeptr     = &size
	isEncrypted = false

	SampleGetFileShares = []fileModel.FileShare{
		{
			Id:                 "3769855c-a102-11e7-b772-17b880d2f537",
			CreatedAt:          "CreatedAt",
			UpdatedAt:          "UpdatedAt",
			Name:               "sample-fileshare-01",
			Description:        "This is first sample fileshare for testing",
			UserId:             "UserID",
			Backend:            "Backend",
			BackendId:          "BackendId",
			Size:               sizeptr,
			Type:               "Type",
			TenantId:           "TenantId",
			Status:             "available",
			Region:             "asia",
			AvailabilityZone:   "default",
			Protocols:          []string{"iscsi"},
			SnapshotId:         "snapshotid",
			Encrypted:          &isEncrypted,
			EncryptionSettings: map[string]string{"foo": "bar"},
		},
	}

	SampleFileShare1 = fileModel.FileShare{
		Id:                 "3769855c-a102-11e7-b772-17b880d2f539",
		CreatedAt:          "CreatedAt",
		UpdatedAt:          "UpdatedAt",
		Name:               "sample-fileshare-01",
		Description:        "This is first sample fileshare for testing",
		UserId:             "UserID",
		Backend:            "Backend",
		BackendId:          "BackendId",
		Size:               sizeptr,
		Type:               "Type",
		TenantId:           "TenantId",
		Status:             "available",
		Region:             "asia",
		AvailabilityZone:   "default",
		Protocols:          []string{"iscsi"},
		SnapshotId:         "snapshotid",
		Encrypted:          &isEncrypted,
		EncryptionSettings: map[string]string{"foo": "bar"},
	}
	SampleFileShare2 = fileModel.FileShare{
		Id:                 "3769855c-a102-11e7-b772-17b880d2f530",
		CreatedAt:          "CreatedAt",
		UpdatedAt:          "UpdatedAt",
		Name:               "sample-fileshare-01",
		Description:        "This is first sample fileshare for testing",
		UserId:             "UserID",
		Backend:            "Backend",
		BackendId:          "BackendId",
		Size:               sizeptr,
		Type:               "Type",
		TenantId:           "TenantId",
		Status:             "available",
		Region:             "asia",
		AvailabilityZone:   "default",
		Protocols:          []string{"iscsi"},
		SnapshotId:         "snapshotid",
		Encrypted:          &isEncrypted,
		EncryptionSettings: map[string]string{"foo": "bar"},
	}

	// var userList []*Users = []*Users{&user1, &user2}
	SampleListFileShares = []*fileModel.FileShare{
		&SampleFileShare1,
		&SampleFileShare2,
	}
)

//=============================Mock Backend Data=====================================
var (
	SampleBackendDetails = []backendModel.Backend{
		{
			Id:         "4769855c-a102-11e7-b772-17b880d2f530",
			TenantId:   "backend-tenantID",
			UserId:     "backend-userID",
			Name:       "backend-name",
			Type:       "backend-type",
			Region:     "backend-region",
			Endpoint:   "backend-endpoint",
			BucketName: "backend-bucketname",
			Access:     "backend-access",
			Security:   "backend-security",
		},
	}
)
