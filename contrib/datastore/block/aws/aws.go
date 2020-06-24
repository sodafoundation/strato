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
	"github.com/opensds/multi-cloud/block/proto"
	"github.com/opensds/multi-cloud/contrib/utils"
	. "github.com/opensds/multi-cloud/s3/error"

	awsec2 "github.com/aws/aws-sdk-go/service/ec2"
	log "github.com/sirupsen/logrus"
)

type AwsAdapter struct {
	session *session.Session
}

func (ad *AwsAdapter) ParseVolume(volumeAWS *awsec2.Volume) (*block.Volume, error) {
	var tags []*block.Tag
	for _, tag := range volumeAWS.Tags {
		tags = append(tags, &block.Tag{
			Key:   *tag.Key,
			Value: *tag.Value,
		})
	}

	meta := make(map[string]interface{})
	meta = map[string]interface{}{
		VolumeId:              *volumeAWS.VolumeId,
		CreationTimeAtBackend: *volumeAWS.CreateTime,
	}

	if volumeAWS.FastRestored != nil {
		meta[FastRestored] = *volumeAWS.FastRestored
	}

	if volumeAWS.FastRestored != nil {
		meta[OutpostArn] = *volumeAWS.OutpostArn
	}

	metadata, err := utils.ConvertMapToStruct(meta)
	if err != nil {
		log.Errorf("failed to convert metadata = [%+v] to struct", metadata, err)
		return nil, err
	}

	volume := &block.Volume{
		Size:               *volumeAWS.Size,
		Encrypted:          *volumeAWS.Encrypted,
		Status:             *volumeAWS.State,
		SnapshotId:         *volumeAWS.SnapshotId,
		Type:               *volumeAWS.VolumeType,
		Tags:               tags,
		Metadata:           metadata,
	}

	if *volumeAWS.VolumeType == "gp2" || *volumeAWS.VolumeType == "io1"{
		volume.Iops = *volumeAWS.Iops
	}

	if volumeAWS.MultiAttachEnabled != nil {
		volume.MultiAttachEnabled = *volumeAWS.MultiAttachEnabled
	}

	if *volumeAWS.Encrypted {
		volume.EncryptionSettings = map[string]string{
			KmsKeyId: *volumeAWS.KmsKeyId,
		}
	}

	return volume, nil
}

func (ad *AwsAdapter) ParseUpdatedVolume(volumeAWS *awsec2.VolumeModification) (*block.Volume, error) {

	meta := map[string]interface{}{
		VolumeId:           *volumeAWS.VolumeId,
		Progress:           *volumeAWS.Progress,
		StartTimeAtBackend: *volumeAWS.StartTime,
	}

	metadata, err := utils.ConvertMapToStruct(meta)
	if err != nil {
		log.Errorf("failed to convert metadata = [%+v] to struct", metadata, err)
		return nil, err
	}

	volume := &block.Volume{
		Size:     *volumeAWS.TargetSize,
		Status:   *volumeAWS.ModificationState,
		Iops:     *volumeAWS.TargetIops,
		Type:     *volumeAWS.TargetVolumeType,
		Metadata: metadata,
	}

	return volume, nil
}

func (ad *AwsAdapter) DescribeVolume(input *awsec2.DescribeVolumesInput) (*awsec2.DescribeVolumesOutput, error) {
	// Create a EC2 client from just a session.
	svc := awsec2.New(ad.session)

	// Get the Volumes
	result, err := svc.DescribeVolumes(input)
	if err != nil {
		log.Errorf("Error in retrieving volume list, err:%v", err)
		return nil, ErrGetFromBackendFailed
	}

	log.Debugf("Describe AWS Volume response = %+v", result)

	return result, nil
}

// Create EBS volume
func (ad *AwsAdapter) CreateVolume(ctx context.Context, volume *block.CreateVolumeRequest) (*block.CreateVolumeResponse, error) {

	// Create a EC2 client from just a session.
	svc := awsec2.New(ad.session)
	var tags []*awsec2.Tag

	for _, tag := range volume.Volume.Tags {
		tags = append(tags, &awsec2.Tag{
			Key:   aws.String(tag.Key),
			Value: aws.String(tag.Value),
		})
	}

	tagList := &awsec2.TagSpecification{
		Tags:         tags,
		ResourceType: aws.String(awsec2.ResourceTypeVolume),
	}

	input := &awsec2.CreateVolumeInput{
		AvailabilityZone:  aws.String(volume.Volume.AvailabilityZone),
		Size:              aws.Int64(volume.Volume.Size),
		VolumeType:        aws.String(volume.Volume.Type),
		TagSpecifications: []*awsec2.TagSpecification{tagList},
		Encrypted:         aws.Bool(volume.Volume.Encrypted),
	}

	if volume.Volume.Type ==  "io1" {
		// Iops is required only for io1 volumes
		input.Iops = aws.Int64(volume.Volume.Iops)

		//Only for specific regions supported
		if volume.Volume.MultiAttachEnabled {
			input.MultiAttachEnabled = aws.Bool(volume.Volume.MultiAttachEnabled)
		}
	}

	if *input.Encrypted {
		input.KmsKeyId = aws.String(volume.Volume.EncryptionSettings[KmsKeyId])
	}

	result, err := svc.CreateVolume(input)
	if err != nil {
		log.Errorf("Error in creating volume: %+v", input, err)
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

	log.Debugf("Create Volume response = %+v", result)

	vol, err := ad.ParseVolume(result)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	return &block.CreateVolumeResponse{
		Volume: vol,
	}, nil
}

func (ad *AwsAdapter) GetVolume(ctx context.Context, volume *block.GetVolumeRequest) (*block.GetVolumeResponse, error) {

	volumeId := aws.String(volume.Volume.Metadata.Fields[VolumeId].GetStringValue())

	volumeIds := []*string{volumeId}
	input := &awsec2.DescribeVolumesInput{
		VolumeIds: volumeIds,
	}

	result, err := ad.DescribeVolume(input)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	log.Infof("Get Volume response = %+v", result)

	vol, err := ad.ParseVolume(result.Volumes[0])

	if err != nil {
		log.Error(err)
		return nil, err
	}

	return &block.GetVolumeResponse{
		Volume: vol,
	}, nil
}

func (ad *AwsAdapter) ListVolume(ctx context.Context, volume *block.ListVolumeRequest) (*block.ListVolumeResponse, error) {
	input := &awsec2.DescribeVolumesInput{}

	result, err := ad.DescribeVolume(input)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	var volumes []*block.Volume
	for _, vol := range result.Volumes {
		fs, err := ad.ParseVolume(vol)
		if err != nil {
			log.Error(err)
			return nil, err
		}
		volumes = append(volumes, fs)
	}

	log.Debugf("List Volumes = %+v", result)

	return &block.ListVolumeResponse{
		Volumes: volumes,
	}, nil
}

func (ad *AwsAdapter) UpdateVolume(ctx context.Context, volume *block.UpdateVolumeRequest) (*block.UpdateVolumeResponse, error) {

	// Create a EC2 client from just a session.
	svc := awsec2.New(ad.session)

	input := &awsec2.ModifyVolumeInput{
		VolumeId:   aws.String(volume.Volume.Metadata.Fields[VolumeId].GetStringValue()),
		Size:       aws.Int64(volume.Volume.Size),
		VolumeType: aws.String(volume.Volume.Type),
		Iops:       aws.Int64(volume.Volume.Iops),
	}

	result, err := svc.ModifyVolume(input)
	if err != nil {
		log.Errorf("Error in updating volume: %+v", input, err)
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

	log.Debugf("Update Volume response = %+v", result)

	vol, err := ad.ParseUpdatedVolume(result.VolumeModification)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	if volume.Volume.Tags == nil || len(volume.Volume.Tags) == 0 {
		return &block.UpdateVolumeResponse{
			Volume: vol,
		}, nil
	}

	var tags []*awsec2.Tag

	for _, tag := range volume.Volume.Tags {
		tags = append(tags, &awsec2.Tag{
			Key:   aws.String(tag.Key),
			Value: aws.String(tag.Value),
		})
	}

	volumeId := aws.String(volume.Volume.Metadata.Fields[VolumeId].GetStringValue())
	resources := []*string{volumeId}

	tagInput := &awsec2.CreateTagsInput{
		Tags:      tags,
		Resources: resources,
	}

	_, err = svc.CreateTags(tagInput)
	if err != nil {
		log.Errorf("Error in updating tags for volume: %+v", input, err)
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
	vol.Tags = volume.Volume.Tags

	return &block.UpdateVolumeResponse{
		Volume: vol,
	}, nil
}

func (ad *AwsAdapter) DeleteVolume(ctx context.Context, volume *block.DeleteVolumeRequest) (*block.DeleteVolumeResponse, error) {

	// Create a EC2 client from just a session.
	svc := awsec2.New(ad.session)

	input := &awsec2.DeleteVolumeInput{
		VolumeId: aws.String(volume.Volume.Metadata.Fields[VolumeId].GetStringValue()),
	}

	result, err := svc.DeleteVolumeRequest(input)
	if err != nil {
		log.Errorf("Error in deleting volume: %+v", input, err)
		return nil, ErrGetFromBackendFailed
	}

	log.Debugf("Delete Volume response = %+v", result)

	return &block.DeleteVolumeResponse{}, nil
}

func (ad *AwsAdapter) Close() error {
	// TODO:
	return nil
}
