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

package gcp

import (
	log "github.com/sirupsen/logrus"
	"github.com/webrtcn/s3client"

	"github.com/opensds/multi-cloud/backend/pkg/utils/constants"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	driver "github.com/opensds/multi-cloud/metadata/pkg/drivers/cloudfactory"
)

type GcpDriverFactory struct {
}

func (cdf *GcpDriverFactory) CreateDriver(backend *backendpb.BackendDetail) (driver.CloudDriver, error) {
	log.Infoln("got the request to create driver in gcp factory")
	endpoint := backend.Endpoint
	AccessKeyID := backend.Access
	AccessKeySecret := backend.Security
	sess := s3client.NewClient(endpoint, AccessKeyID, AccessKeySecret)
	adap := &GcpAdapter{backend: backend, session: sess}

	return adap, nil
}

func init() {
	driver.RegisterDriverFactory(constants.BackendTypeGcs, &GcpDriverFactory{})
}
