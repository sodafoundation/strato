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

package aws

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/micro/go-micro/v2/util/log"

	"github.com/opensds/multi-cloud/backend/pkg/utils/constants"
	"github.com/opensds/multi-cloud/contrib/datastore/block/driver"

	backendpb "github.com/opensds/multi-cloud/backend/proto"
)

type AwsBlockDriverFactory struct {
}

func (factory *AwsBlockDriverFactory) CreateBlockStorageDriver(backend *backendpb.BackendDetail) (driver.BlockDriver, error) {
	log.Infof("Entered to create aws volume driver")

	// Create AWS session with the AWS cloud credentials
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(backend.Region),
		Credentials: credentials.NewStaticCredentials(backend.Access, backend.Security, ""),
	})
	if err != nil {
		log.Errorf("Error in getting the Session")
		return nil, err
	}

	adapter := &AwsAdapter{session: sess}
	return adapter, nil
}

func init() {
	driver.RegisterDriverFactory(constants.BackendTypeAwsBlock, &AwsBlockDriverFactory{})
}
