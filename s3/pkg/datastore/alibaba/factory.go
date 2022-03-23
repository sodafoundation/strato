// Copyright 2019 The soda Authors.
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
package alibaba

import (
	"github.com/aliyun/aliyun-oss-go-sdk/oss"

	"github.com/soda/multi-cloud/backend/pkg/utils/constants"
	backendpb "github.com/soda/multi-cloud/backend/proto"
	"github.com/soda/multi-cloud/s3/pkg/datastore/driver"
)

type AlibabaDriverFactory struct {
}

func (factory *AlibabaDriverFactory) CreateDriver(backend *backendpb.BackendDetail) (driver.StorageDriver, error) {
	endpoint := backend.Endpoint
	AccessKeyID := backend.Access
	AccessKeySecret := backend.Security

	client, err := oss.New(endpoint, AccessKeyID, AccessKeySecret)
	if err != nil {
		return nil, err
	}
	adap := &OSSAdapter{backend: backend, client: client}

	return adap, nil
}

func init() {
	driver.RegisterDriverFactory(constants.BackendTypeAlibaba, &AlibabaDriverFactory{})
}
