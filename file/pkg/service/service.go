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

package service

import (
	"context"

	"github.com/micro/go-micro/v2/client"
	"github.com/opensds/multi-cloud/file/pkg/db"
	"github.com/opensds/multi-cloud/file/pkg/utils"
	pb "github.com/opensds/multi-cloud/file/proto"
	log "github.com/sirupsen/logrus"
)

type fileService struct {
	fileClient pb.FileService
}

func NewFileService() pb.FileHandler {

	log.Infof("Init file service finished.\n")
	return &fileService{
		fileClient: pb.NewFileService("file", client.DefaultClient),
	}
}

func (f *fileService) ListFileShare(ctx context.Context, in *pb.ListFileShareRequest, out *pb.ListFileShareResponse) error {
	log.Info("Received ListFileShare request.")

	res, err := db.DbAdapter.ListFileShare(ctx, int(in.Limit), int(in.Offset), in.Filter)
	if err != nil {
		log.Errorf("Failed to List FileShares: %v\n", err)
		return err
	}

	var fileshares []*pb.FileShare
	for _, fs := range res {
		var tags []*pb.Tag
		for _, tag := range fs.Tags {
			tags = append(tags, &pb.Tag{
				Key:   tag.Key,
				Value: tag.Value,
			})
		}
		fileshares = append(fileshares, &pb.FileShare{
			Id:               fs.Id.Hex(),
			CreatedAt:        fs.CreatedAt,
			UpdatedAt:        fs.UpdatedAt,
			Name:             fs.Name,
			Description:      fs.Description,
			TenantId:         fs.TenantId,
			UserId:           fs.UserId,
			BackendId:        fs.BackendId,
			Backend:          fs.Backend,
			Size:             fs.Size,
			Type:             fs.Type,
			Status:           fs.Status,
			Region:           fs.Region,
			AvailabilityZone: fs.AvailabilityZone,
			Tags:             tags,
			Protocols:        fs.Protocols,
			SnapshotId:       fs.SnapshotId,
			Encrypted:        fs.Encrypted,
			Metadata:         fs.Metadata,
		})
	}
	out.Fileshares = fileshares
	out.Next = in.Offset + int32(len(res))

	log.Debugf("Listed File Share successfully, #num=%d, fileshares: %+v\n", len(fileshares), fileshares)
	return nil
}

func (f *fileService) GetFileShare(ctx context.Context, in *pb.GetFileShareRequest, out *pb.GetFileShareResponse) error {
	log.Info("Received GetFileShare request.")

	fs, err := db.DbAdapter.GetFileShare(ctx, in.Id)
	if err != nil {
		log.Errorf("failed to get fileshare: %v\n", err)
		return err
	}
	var tags []*pb.Tag
	for _, tag := range fs.Tags {
		tags = append(tags, &pb.Tag{
			Key:   tag.Key,
			Value: tag.Value,
		})
	}
	out.Fileshare = &pb.FileShare{
		Id:                 fs.Id.Hex(),
		CreatedAt:          fs.CreatedAt,
		UpdatedAt:          fs.UpdatedAt,
		Name:               fs.Name,
		Description:        fs.Description,
		TenantId:           fs.TenantId,
		UserId:             fs.UserId,
		BackendId:          fs.BackendId,
		Backend:            fs.Backend,
		Size:               fs.Size / utils.GB_FACTOR,
		Type:               fs.Type,
		Status:             fs.Status,
		Region:             fs.Region,
		AvailabilityZone:   fs.AvailabilityZone,
		Tags:               tags,
		Protocols:          fs.Protocols,
		SnapshotId:         fs.SnapshotId,
		Encrypted:          fs.Encrypted,
		EncryptionSettings: fs.EncryptionSettings,
		Metadata:           fs.Metadata,
	}
	log.Info("Get file share successfully.")
	return nil
}
