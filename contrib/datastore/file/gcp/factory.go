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

package gcp

import (
	"context"

	"github.com/micro/go-micro/v2/util/log"
	"github.com/opensds/multi-cloud/api/pkg/filters/signature/credentials/keystonecredentials"
	"github.com/opensds/multi-cloud/api/pkg/utils/cryptography"
	"github.com/opensds/multi-cloud/backend/pkg/utils/constants"
	"github.com/opensds/multi-cloud/contrib/datastore/file/driver"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/option"

	backendpb "github.com/opensds/multi-cloud/backend/proto"
	gcpfilev1 "google.golang.org/api/file/v1"
)

type GcpFSDriverFactory struct {
}

func (factory *GcpFSDriverFactory) CreateFileStorageDriver(backend *backendpb.BackendDetail) (driver.FileStorageDriver, error) {
	log.Infof("Entered to create gcp file share driver")

	ctx := context.Background()

	credential, err := keystonecredentials.NewCredentialsClient(backend.Access).Get()
	if err != nil {
		return nil, err
	}
	aes := cryptography.NewSymmetricKeyEncrypter("aes")
	decrypted, err := aes.Decrypter(backend.Security, []byte(credential.SecretAccessKey))
	if err != nil {
		return nil, err
	}

	jsonData := []byte(decrypted)
	credentials, err := google.CredentialsFromJSON(ctx, jsonData, compute.CloudPlatformScope)
	if err != nil {
		return nil, err
	}
	client := oauth2.NewClient(ctx, credentials.TokenSource)

	fileService, err := gcpfilev1.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return nil, err
	}

	adapter := &GcpAdapter{
		backend:           backend,
		fileService:       fileService,
		instancesService:  gcpfilev1.NewProjectsLocationsInstancesService(fileService),
		operationsService: gcpfilev1.NewProjectsLocationsOperationsService(fileService),
	}

	return adapter, nil
}

func init() {
	driver.RegisterDriverFactory(constants.BackendTypeGcsFile, &GcpFSDriverFactory{})
}
