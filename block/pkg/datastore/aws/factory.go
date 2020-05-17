// Copyright 2020 The OpenSDS Authors.
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
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	"github.com/opensds/multi-cloud/block/pkg/datastore/driver"
	log "github.com/sirupsen/logrus"
)

type AwsDriverFactory struct {
}

func (factory *AwsDriverFactory) CreateDriver(backend *backendpb.BackendDetail) (driver.StorageDriver, error) {
    log.Infof("Entered to create driver")
	AccessKeyID := backend.Access
	AccessKeySecret := backend.Security
	region := backend.Region

    // Create AWS session with the AWS cloud credentials
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(AccessKeyID, AccessKeySecret, ""),
	})
	if err != nil {
        // TODO: Add AWSErrors
		log.Errorf("Error in getting the Session")
		return nil, err
	}

	adap := &AwsAdapter{session: sess}
	return adap, nil
}

func init() {
	driver.RegisterDriverFactory("aws-block", &AwsDriverFactory{})
}
