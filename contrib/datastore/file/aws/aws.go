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

// Reference: https://docs.aws.amazon.com/sdk-for-go/api/service/efs/

package aws

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/efs"
	"github.com/opensds/multi-cloud/contrib/utils"

	pb "github.com/opensds/multi-cloud/file/proto"
	log "github.com/sirupsen/logrus"
)

type AwsAdapter struct {
	session *session.Session
}

func (ad *AwsAdapter) ParseFileShare(fsDesc *efs.FileSystemDescription) (*pb.FileShare, error) {
	var tags []*pb.Tag
	for _, tag := range fsDesc.Tags {
		tags = append(tags, &pb.Tag{
			Key:   *tag.Key,
			Value: *tag.Value,
		})
	}

	meta := map[string]interface{}{
		FileSystemName:        *fsDesc.Name,
		FileSystemId:          *fsDesc.FileSystemId,
		OwnerId:               *fsDesc.OwnerId,
		FileSystemSize:        *fsDesc.SizeInBytes,
		ThroughputMode:        *fsDesc.ThroughputMode,
		PerformanceMode:       *fsDesc.PerformanceMode,
		CreationToken:         *fsDesc.CreationToken,
		CreationTimeAtBackend: *fsDesc.CreationTime,
		LifeCycleState:        *fsDesc.LifeCycleState,
		NumberOfMountTargets:  *fsDesc.NumberOfMountTargets,
	}

	if *fsDesc.ThroughputMode == efs.ThroughputModeProvisioned {
		meta[ProvisionedThroughputInMibps] = *fsDesc.ProvisionedThroughputInMibps
	}

	metadata, err := utils.ConvertMapToStruct(meta)
	if err != nil {
		log.Errorf("failed to convert metadata = [%+v] to struct", metadata, err)
		return nil, err
	}

	fileshare := &pb.FileShare{
		Name:      *fsDesc.Name,
		Size:      *fsDesc.SizeInBytes.Value,
		Encrypted: *fsDesc.Encrypted,
		Status:    *fsDesc.LifeCycleState,
		Tags:      tags,
		Metadata:  metadata,
	}

	if *fsDesc.Encrypted {
		fileshare.EncryptionSettings = map[string]string{
			KmsKeyId: *fsDesc.KmsKeyId,
		}
	}

	return fileshare, nil
}

func (ad *AwsAdapter) ParseUpdateFileShare(fsUpdate *efs.UpdateFileSystemOutput) (*pb.FileShare, error) {
	var tags []*pb.Tag
	for _, tag := range fsUpdate.Tags {
		tags = append(tags, &pb.Tag{
			Key:   *tag.Key,
			Value: *tag.Value,
		})
	}

	meta := map[string]interface{}{
		FileSystemName:        *fsUpdate.Name,
		FileSystemId:          *fsUpdate.FileSystemId,
		OwnerId:               *fsUpdate.OwnerId,
		FileSystemSize:        *fsUpdate.SizeInBytes,
		ThroughputMode:        *fsUpdate.ThroughputMode,
		PerformanceMode:       *fsUpdate.PerformanceMode,
		CreationToken:         *fsUpdate.CreationToken,
		CreationTimeAtBackend: *fsUpdate.CreationTime,
		LifeCycleState:        *fsUpdate.LifeCycleState,
		NumberOfMountTargets:  *fsUpdate.NumberOfMountTargets,
	}

	if *fsUpdate.ThroughputMode == efs.ThroughputModeProvisioned {
		meta[ProvisionedThroughputInMibps] = *fsUpdate.ProvisionedThroughputInMibps
	}

	metadata, err := utils.ConvertMapToStruct(meta)
	if err != nil {
		log.Errorf("Failed to convert metadata = [%+v] to struct", metadata, err)
		return nil, err
	}

	fileshare := &pb.FileShare{
		Size:      *fsUpdate.SizeInBytes.Value,
		Encrypted: *fsUpdate.Encrypted,
		Status:    *fsUpdate.LifeCycleState,
		Tags:      tags,
		Metadata:  metadata,
	}

	if *fsUpdate.Encrypted {
		fileshare.EncryptionSettings = map[string]string{
			KmsKeyId: *fsUpdate.KmsKeyId,
		}
	}

	return fileshare, nil
}

func (ad *AwsAdapter) DescribeFileShare(input *efs.DescribeFileSystemsInput) (*efs.DescribeFileSystemsOutput, error) {
	// Create a EFS client from just a session.
	svc := efs.New(ad.session)

	result, err := svc.DescribeFileSystems(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case efs.ErrCodeBadRequest:
				log.Errorf(efs.ErrCodeBadRequest, aerr.Error())
			case efs.ErrCodeInternalServerError:
				log.Errorf(efs.ErrCodeInternalServerError, aerr.Error())
			case efs.ErrCodeFileSystemNotFound:
				log.Errorf(efs.ErrCodeFileSystemNotFound, aerr.Error())
			default:
				log.Errorf(aerr.Error())
			}
		} else {
			log.Error(err)
		}
		return nil, err
	}
	log.Debugf("Describe AWS File System response = %+v", result)

	return result, nil
}

func (ad *AwsAdapter) CreateFileShare(ctx context.Context, fs *pb.CreateFileShareRequest) (*pb.CreateFileShareResponse, error) {
	// Create a EFS client from just a session.
	svc := efs.New(ad.session)

	var tags []*efs.Tag
	for _, tag := range fs.Fileshare.Tags {
		tags = append(tags, &efs.Tag{
			Key:   aws.String(tag.Key),
			Value: aws.String(tag.Value),
		})
	}

	creationToken := utils.RandString(36)

	input := &efs.CreateFileSystemInput{
		CreationToken:   aws.String(creationToken),
		Encrypted:       aws.Bool(fs.Fileshare.Encrypted),
		PerformanceMode: aws.String(fs.Fileshare.Metadata.Fields[PerformanceMode].GetStringValue()),
		Tags:            tags,
		ThroughputMode:  aws.String(fs.Fileshare.Metadata.Fields[ThroughputMode].GetStringValue()),
	}

	if *input.ThroughputMode == efs.ThroughputModeProvisioned {
		input.ProvisionedThroughputInMibps = aws.Float64(fs.Fileshare.Metadata.Fields[ProvisionedThroughputInMibps].GetNumberValue())
	}

	if *input.Encrypted {
		input.KmsKeyId = aws.String(fs.Fileshare.EncryptionSettings["KmsKeyId"])
	}

	result, err := svc.CreateFileSystem(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case efs.ErrCodeBadRequest:
				log.Errorf(efs.ErrCodeBadRequest, aerr.Error())
			case efs.ErrCodeInternalServerError:
				log.Errorf(efs.ErrCodeInternalServerError, aerr.Error())
			case efs.ErrCodeFileSystemAlreadyExists:
				log.Errorf(efs.ErrCodeFileSystemAlreadyExists, aerr.Error())
			case efs.ErrCodeFileSystemLimitExceeded:
				log.Errorf(efs.ErrCodeFileSystemLimitExceeded, aerr.Error())
			case efs.ErrCodeInsufficientThroughputCapacity:
				log.Errorf(efs.ErrCodeInsufficientThroughputCapacity, aerr.Error())
			case efs.ErrCodeThroughputLimitExceeded:
				log.Errorf(efs.ErrCodeThroughputLimitExceeded, aerr.Error())
			default:
				log.Errorf(aerr.Error())
			}
		} else {
			log.Error(err)
		}
		return nil, err
	}

	log.Debugf("Create File share response = %+v", result)

	fileShare, err := ad.ParseFileShare(result)

	if err != nil {
		log.Error(err)
		return nil, err
	}

	return &pb.CreateFileShareResponse{
		Fileshare: fileShare,
	}, nil
}

func (ad *AwsAdapter) GetFileShare(ctx context.Context, fs *pb.GetFileShareRequest) (*pb.GetFileShareResponse, error) {

	input := &efs.DescribeFileSystemsInput{
		FileSystemId: aws.String(fs.Fileshare.Metadata.Fields[FileSystemId].GetStringValue()),
	}

	result, err := ad.DescribeFileShare(input)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	log.Infof("Get File share response = %+v", result)

	fileShare, err := ad.ParseFileShare(result.FileSystems[0])

	if err != nil {
		log.Error(err)
		return nil, err
	}

	return &pb.GetFileShareResponse{
		Fileshare: fileShare,
	}, nil
}

func (ad *AwsAdapter) ListFileShare(ctx context.Context, in *pb.ListFileShareRequest) (*pb.ListFileShareResponse, error) {

	input := &efs.DescribeFileSystemsInput{}

	result, err := ad.DescribeFileShare(input)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	var fileshares []*pb.FileShare
	for _, fileshare := range result.FileSystems {
		fs, err := ad.ParseFileShare(fileshare)
		if err != nil {
			log.Error(err)
			return nil, err
		}
		fs.Name = *fileshare.Name
		fileshares = append(fileshares, fs)
	}

	log.Debugf("List File shares = %+v", result)

	return &pb.ListFileShareResponse{
		Fileshares: fileshares,
	}, nil
}

func (ad *AwsAdapter) UpdatefileShare(ctx context.Context, in *pb.UpdateFileShareRequest) (*pb.UpdateFileShareResponse, error) {
	// Create a EFS client from just a session.
	svc := efs.New(ad.session)

	input := &efs.UpdateFileSystemInput{
		FileSystemId:   aws.String(in.Fileshare.Metadata.Fields[FileSystemId].GetStringValue()),
		ThroughputMode: aws.String(in.Fileshare.Metadata.Fields[ThroughputMode].GetStringValue()),
	}

	if *input.ThroughputMode == efs.ThroughputModeProvisioned {
		input.ProvisionedThroughputInMibps = aws.Float64(in.Fileshare.Metadata.Fields[ProvisionedThroughputInMibps].GetNumberValue())
	}

	result, err := svc.UpdateFileSystem(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case efs.ErrCodeBadRequest:
				log.Errorf(efs.ErrCodeBadRequest, aerr.Error())
			case efs.ErrCodeFileSystemNotFound:
				log.Errorf(efs.ErrCodeFileSystemNotFound, aerr.Error())
			case efs.ErrCodeIncorrectFileSystemLifeCycleState:
				log.Errorf(efs.ErrCodeIncorrectFileSystemLifeCycleState, aerr.Error())
			case efs.ErrCodeInsufficientThroughputCapacity:
				log.Errorf(efs.ErrCodeInsufficientThroughputCapacity, aerr.Error())
			case efs.ErrCodeInternalServerError:
				log.Errorf(efs.ErrCodeInternalServerError, aerr.Error())
			case efs.ErrCodeFileSystemLimitExceeded:
				log.Errorf(efs.ErrCodeFileSystemLimitExceeded, aerr.Error())
			case efs.ErrCodeThroughputLimitExceeded:
				log.Errorf(efs.ErrCodeThroughputLimitExceeded, aerr.Error())
			case efs.ErrCodeTooManyRequests:
				log.Errorf(efs.ErrCodeTooManyRequests, aerr.Error())
			default:
				log.Errorf(aerr.Error())
			}
		} else {
			log.Error(err)
		}
		return nil, err
	}

	log.Debugf("Update File share response = %+v", result)

	fileShare, err := ad.ParseUpdateFileShare(result)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	if in.Fileshare.Tags == nil || len(in.Fileshare.Tags) == 0 {
		return &pb.UpdateFileShareResponse{
			Fileshare: fileShare,
		}, nil
	}

	var tags []*efs.Tag
	for _, tag := range in.Fileshare.Tags {
		tags = append(tags, &efs.Tag{
			Key:   aws.String(tag.Key),
			Value: aws.String(tag.Value),
		})
	}

	tagInput := &efs.TagResourceInput{
		ResourceId: aws.String(in.Fileshare.Metadata.Fields[FileSystemId].GetStringValue()),
		Tags:       tags,
	}

	_, err = svc.TagResource(tagInput)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case efs.ErrCodeBadRequest:
				log.Errorf(efs.ErrCodeBadRequest, aerr.Error())
			case efs.ErrCodeFileSystemNotFound:
				log.Errorf(efs.ErrCodeFileSystemNotFound, aerr.Error())
			case efs.ErrCodeInternalServerError:
				log.Errorf(efs.ErrCodeInternalServerError, aerr.Error())
			case efs.ErrCodeAccessPointNotFound:
				log.Errorf(efs.ErrCodeAccessPointNotFound, aerr.Error())
			default:
				log.Errorf(aerr.Error())
			}
		} else {
			log.Error(err)
		}
		return nil, err
	}

	fileShare.Tags = in.Fileshare.Tags

	return &pb.UpdateFileShareResponse{
		Fileshare: fileShare,
	}, nil
}

func (ad *AwsAdapter) DeleteFileShare(ctx context.Context, in *pb.DeleteFileShareRequest) (*pb.DeleteFileShareResponse, error) {
	svc := efs.New(ad.session)

	input := &efs.DeleteFileSystemInput{FileSystemId: aws.String(in.Fileshare.Metadata.Fields[FileSystemId].GetStringValue())}

	result, err := svc.DeleteFileSystem(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case efs.ErrCodeBadRequest:
				log.Errorf(efs.ErrCodeBadRequest, aerr.Error())
			case efs.ErrCodeInternalServerError:
				log.Errorf(efs.ErrCodeInternalServerError, aerr.Error())
			case efs.ErrCodeFileSystemNotFound:
				log.Errorf(efs.ErrCodeFileSystemNotFound, aerr.Error())
			case efs.ErrCodeFileSystemInUse:
				log.Errorf(efs.ErrCodeFileSystemInUse, aerr.Error())
			default:
				log.Errorf(aerr.Error())
			}
		} else {
			log.Error(err)
		}
		return nil, err
	}

	log.Debugf("Delete File share response = %+v", result)

	return &pb.DeleteFileShareResponse{}, nil
}

func (ad *AwsAdapter) Close() error {
	// TODO:
	return nil
}
