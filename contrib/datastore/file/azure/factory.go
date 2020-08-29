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

package azure

import (
	"github.com/micro/go-micro/v2/util/log"
	"github.com/opensds/multi-cloud/backend/pkg/utils/constants"
	"github.com/opensds/multi-cloud/contrib/datastore/drivers"

	backendpb "github.com/opensds/multi-cloud/backend/proto"
)

type AzureFSDriverFactory struct {
}

func (factory *AzureFSDriverFactory) CreateFileStorageDriver(backend *backendpb.BackendDetail) (driver.FileStorageDriver, error) {
	log.Infof("Entered to create azure file share driver")

	backend.Endpoint = "https://" + backend.Access + ".file.core.windows.net/"

	adapter := &AzureAdapter{backend: backend}

	return adapter, nil
}

func init() {
	driver.RegisterDriverFactory(constants.BackendTypeAzureFile, &AzureFSDriverFactory{})
}
