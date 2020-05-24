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
	"context"
	"github.com/aws/aws-sdk-go/aws/session"
	awsec2 "github.com/aws/aws-sdk-go/service/ec2"
	dscommon "github.com/opensds/multi-cloud/block/pkg/datastore/common"
	pb "github.com/opensds/multi-cloud/block/proto"
	. "github.com/opensds/multi-cloud/s3/error"
	log "github.com/sirupsen/logrus"
)

type AwsAdapter struct {
	session *session.Session
}

func (ad *AwsAdapter) List(ctx context.Context) (*pb.ListVolumesResponse, error) {
	getVolumesInput := &awsec2.DescribeVolumesInput{}
	var result pb.ListVolumesResponse
	log.Infof("Getting volumes list from AWS EC2 service")

	// Create a session of EC2
	svc := awsec2.New(ad.session)

	// Get the Volumes
	volResponse, err := svc.DescribeVolumes(getVolumesInput)
	if err != nil {
		log.Errorf("Errror in getting volumes list, err:%v", err)
		return nil, ErrGetFromBackendFailed
	}
	log.Infof("Describe volumes from AWS succeeded")
	for _, vol := range volResponse.Volumes {
		result.Volumes = append(result.Volumes, &pb.Volume{
			Id: *vol.VolumeId,
			// Always report size in Bytes
			Size:      (*vol.Size) * (dscommon.GB_FACTOR),
			Type:      *vol.VolumeType,
			Status:    *vol.State,
			Encrypted: *vol.Encrypted,
		})
	}
	log.Infof("Successfully got the volumes list")
	return &result, nil
}
