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
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	awsec2 "github.com/aws/aws-sdk-go/service/ec2"
	dscommon "github.com/opensds/multi-cloud/block/pkg/datastore/common"
	"github.com/opensds/multi-cloud/block/pkg/model"
	pb "github.com/opensds/multi-cloud/block/proto"
	. "github.com/opensds/multi-cloud/s3/error"
	log "github.com/sirupsen/logrus"
)

type AwsAdapter struct {
	session *session.Session
}

// Create EBS volume
func (ad *AwsAdapter) Create(ctx context.Context, volume *model.Volume) (*pb.CreateVolumeResponse, error) {
	var result pb.CreateVolumeResponse
	var tags []*awsec2.Tag

	tagList := &awsec2.TagSpecification{
		Tags: append(tags, &awsec2.Tag{
			Key:   aws.String("Name"),
			Value: aws.String(volume.Name)}),
		ResourceType: aws.String(awsec2.ResourceTypeVolume),
	}

	volCreationInput := &awsec2.CreateVolumeInput{
		AvailabilityZone:  aws.String(volume.AvailabilityZone),
		Size:              aws.Int64(volume.Size),
		VolumeType:        aws.String(volume.Type),
		TagSpecifications: []*awsec2.TagSpecification{tagList},
	}
	if volume.Type == "io1" {
		// Iops is required only for io1 volumes
		volCreationInput.Iops = aws.Int64(volume.Iops)
	}
	// Create a session of EC2
	svc := awsec2.New(ad.session)

	// Get the Volumes
	volResponse, err := svc.CreateVolume(volCreationInput)
	if err != nil {
		log.Errorf("errror in creating volume, err:%v", err)
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				log.Errorf(aerr.Error())
			}
		} else {
			log.Errorf(err.Error())
		}
		return nil, ErrGetFromBackendFailed
	}

	log.Infof("Create volumes from AWS succeeded %v", volResponse)
	var volName string
	for _, tag := range volResponse.Tags {
		if *tag.Key == "Name" {
			volName = *tag.Value
		}
	}
	result.Volume = &pb.Volume{
		Type:             *volResponse.VolumeType,
		Size:             (*volResponse.Size) * (dscommon.GB_FACTOR),
		VolumeId:         *volResponse.VolumeId,
		Encrypted:        *volResponse.Encrypted,
		CreatedAt:        (*volResponse.CreateTime).String(),
		AvailabilityZone: *volResponse.AvailabilityZone,
		State:            *volResponse.State,
		Name:             volName,
	}
	if *volResponse.VolumeType == "io1" {
		result.Volume.Iops = *volResponse.Iops
	}
	return &result, nil
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
		log.Errorf("errror in getting volumes list, err:%v", err)
		return nil, ErrGetFromBackendFailed
	}
	log.Infof("Describe volumes from AWS succeeded %v", volResponse)

	for _, vol := range volResponse.Volumes {
		var volName string
		for _, tag := range vol.Tags {
			if *tag.Key == "Name" {
				volName = *tag.Value
			}
		}
		result.Volumes = append(result.Volumes, &pb.Volume{
			VolumeId: *vol.VolumeId,
			// Always report size in Bytes
			Size:             (*vol.Size) * (dscommon.GB_FACTOR),
			Type:             *vol.VolumeType,
			State:            *vol.State,
			AvailabilityZone: *vol.AvailabilityZone,
			CreatedAt:        (*vol.CreateTime).String(),
			Name:             volName,
		})
	}
	log.Infof("Successfully got the volumes list")
	return &result, nil
}
