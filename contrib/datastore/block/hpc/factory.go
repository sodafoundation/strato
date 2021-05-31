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

package hpc

import (
	"errors"
	"strings"

	"github.com/opensds/multi-cloud/backend/pkg/utils/constants"
	backend "github.com/opensds/multi-cloud/backend/proto"
	"github.com/opensds/multi-cloud/contrib/datastore/block/driver"

	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/auth/basic"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/config"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/httphandler"
	evs "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/evs/v2"
	"github.com/micro/go-micro/v2/util/log"
)

type HpcBlockDriverFactory struct {
}

func (factory *HpcBlockDriverFactory) CreateBlockStorageDriver(backend *backend.BackendDetail) (driver.BlockDriver, error) {
	log.Infof("Entered to create HPC volume driver")

	// Getting region and projectId together as project ID and region are always associated
	region_prjid := strings.Split(backend.Region, ":")

	if len(region_prjid) != 2 {
		err := "Incorrect region and projectID info"
		log.Error(err)
		return nil, errors.New(err)
	}
	// Create HPC session with the HPC cloud credentials
	// https://developer.huaweicloud.com/en-us/endpoint?all
	url := "https://evs." + region_prjid[0] + ".myhuaweicloud.com"
	client := evs.NewEvsClient(
		evs.EvsClientBuilder().
			WithEndpoint(url).
			WithCredential(
				basic.NewCredentialsBuilder().
					WithAk(backend.Access).
					WithSk(backend.Security).
					WithProjectId(region_prjid[1]).
					Build()).
			WithHttpConfig(config.DefaultHttpConfig().
				WithIgnoreSSLVerification(true).
				WithHttpHandler(httphandler.
					NewHttpHandler())).
			Build())

	adapter := &HpcAdapter{evsc: client}
	return adapter, nil
}

func init() {
	driver.RegisterDriverFactory(constants.BackendTypeHpcBlock, &HpcBlockDriverFactory{})
}
