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

package huawei

import (
	"github.com/huaweicloud/golangsdk"
	"github.com/huaweicloud/golangsdk/openstack"
	"github.com/micro/go-micro/v2/util/log"

	"github.com/opensds/multi-cloud/backend/pkg/utils/constants"
	"github.com/opensds/multi-cloud/contrib/datastore/file/driver"

	backendpb "github.com/opensds/multi-cloud/backend/proto"
)

type HwSFSDriverFactory struct {
}

func (factory *HwSFSDriverFactory) CreateFileStorageDriver(backend *backendpb.BackendDetail) (driver.FileStorageDriver, error) {
	log.Infof("Entered to create hw file share driver")

	identityEndpoint := "https://iam." + backend.Region + ".myhwclouds.com/v3"

	ao := golangsdk.AKSKAuthOptions{
		IdentityEndpoint: identityEndpoint,
		AccessKey:        backend.Access,
		SecretKey:        backend.Security,
		ProjectName:      backend.Region,
	}

	provider, err := openstack.NewClient(ao.IdentityEndpoint)
	if err != nil {
		return nil, err
	}

	err = openstack.Authenticate(provider, ao)
	if err != nil {
		return nil, err
	}

	client, err := openstack.NewHwSFSV2(provider, golangsdk.EndpointOpts{
		Region: backend.Region,
	})
	if err != nil {
		log.Errorf("create client[Hw File Share] failed, err:%v\n", err)
		return nil, err
	}

	adapter := &HwFSAdapter{client: client}
	return adapter, nil
}

func init() {
	driver.RegisterDriverFactory(constants.BackendTypeHwSFS, &HwSFSDriverFactory{})
}
