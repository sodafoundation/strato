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

/*
This package includes a collection of fake stuffs for testing work.
*/

package collection

import (
	backendModel "github.com/opensds/multi-cloud/backend/pkg/model"
)

var (
	SampleBackends = []backendModel.Backend{
		{
			Id:         "4769855c-a102-11e7-b772-17b880d2f530",
			TenantId:   "sample-backend-tenantID",
			UserId:     "sample-backend-userID",
			Name:       "sample-backend-name",
			Type:       "sample-backend-type",
			Region:     "sample-backend-region",
			Endpoint:   "sample-backend-endpoint",
			BucketName: "sample-backend-bucketname",
			Access:     "sample-backend-access",
			Security:   "sample-backend-security",
		},
	}
	SampleCreateBackend = []backendModel.Backend{
		{
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
		},
	}
)
