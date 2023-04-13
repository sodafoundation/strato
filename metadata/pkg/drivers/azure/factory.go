// Copyright 2023 The SODA Authors.
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
	"net/url"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/opensds/multi-cloud/backend/pkg/utils/constants"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	driver "github.com/opensds/multi-cloud/metadata/pkg/drivers/cloudfactory"
)

type AzureBlobDriverFactory struct {
}

func (factory *AzureBlobDriverFactory) CreateDriver(backend *backendpb.BackendDetail) (driver.CloudDriver, error) {
	endpoint := backend.Endpoint
	AccessKeyID := backend.Access
	AccessKeySecret := backend.Security

	credential, err := azblob.NewSharedKeyCredential(AccessKeyID, AccessKeySecret)

	if err != nil {
		return nil, err
	}

	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	u, _ := url.Parse(endpoint)

	serviceURL := azblob.NewServiceURL(*u, p)

	adap := &AzureAdapter{backend: backend, serviceURL: serviceURL}

	return adap, nil
}

func init() {
	driver.RegisterDriverFactory(constants.BackendTypeAzure, &AzureBlobDriverFactory{})
}
