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
	. "github.com/opensds/multi-cloud/s3/error"
	dscommon "github.com/opensds/multi-cloud/block/pkg/datastore/common"
	//"github.com/opensds/multi-cloud/block/pkg/datastore/common"
	log "github.com/sirupsen/logrus"
)

type AwsAdapter struct {
	session *session.Session
}

type s3Cred struct {
	ak string
	sk string
}

func (ad *AwsAdapter) List(ctx context.Context) ([]dscommon.ListVolumes, error) {
	getVolumesInput := &awsec2.DescribeVolumesInput{}
	log.Infof("Getting volumes list from AWS EC2 service")

	svc := awsec2.New(ad.session)
	volResponse, err := svc.DescribeVolumes(getVolumesInput)
	if err != nil {
		log.Errorf("Errror in getting volumes list, err:%v",  err)
		return nil, ErrGetFromBackendFailed
	}

	log.Infof("Describe volumes from AWS succeeded")
    volume := dscommon.ListVolumes{}
    var volumes []dscommon.ListVolumes
    for _, vol := range volResponse.Volumes {
            //volume.Name = vol.VolumeName
        volume.VolId = *vol.VolumeId
        // Always report size in GB
        volume.VolSize = (*vol.Size) * (dscommon.GB_FACTOR)
        volume.VolType = *vol.VolumeType
        volume.VolStatus = *vol.State
        //volume.VolMultiAttachEnabled = *vol.MultiAttachEnabled
        //volume.VolEncrypted = *vol.Encrypted
        volumes = append(volumes, volume)
	}
    log.Infof("Successfully got the volumes list")
	return volumes, nil
}

func (ad *AwsAdapter) Close() error {
	// TODO:
	return nil
}
