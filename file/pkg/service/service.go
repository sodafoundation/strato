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
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/globalsign/mgo/bson"
	"github.com/micro/go-micro/v2/client"
	log "github.com/sirupsen/logrus"

	"github.com/soda/multi-cloud/backend/pkg/utils/constants"
	backend "github.com/soda/multi-cloud/backend/proto"
	"github.com/soda/multi-cloud/contrib/datastore/file/aws"
	"github.com/soda/multi-cloud/contrib/datastore/file/driver"
	driverutils "github.com/soda/multi-cloud/contrib/utils"
	"github.com/soda/multi-cloud/file/pkg/db"
	"github.com/soda/multi-cloud/file/pkg/model"
	"github.com/soda/multi-cloud/file/pkg/utils"
	pb "github.com/soda/multi-cloud/file/proto"
)

const (
	MICRO_ENVIRONMENT = "MICRO_ENVIRONMENT"
	K8S               = "k8s"

	fileService_Docker = "file"
	fileService_K8S    = "soda.multicloud.v1.file"

	backendService_Docker = "backend"
	backendService_K8S    = "soda.multicloud.v1.backend"
)

type fileService struct {
	fileClient    pb.FileService
	backendClient backend.BackendService
}

func NewFileService() pb.FileHandler {

	log.Infof("Init file service finished.\n")

	flService := fileService_Docker
	backendService := backendService_Docker

	if os.Getenv(MICRO_ENVIRONMENT) == K8S {
		flService = fileService_K8S
		backendService = backendService_K8S
	}

	return &fileService{
		fileClient:    pb.NewFileService(flService, client.DefaultClient),
		backendClient: backend.NewBackendService(backendService, client.DefaultClient),
	}
}

func (f *fileService) ListFileShare(ctx context.Context, in *pb.ListFileShareRequest, out *pb.ListFileShareResponse) error {
	log.Info("Received ListFileShare request.")

	if in.Limit < 0 || in.Offset < 0 {
		msg := fmt.Sprintf("Invalid pagination parameter, limit = %d and offset = %d.", in.Limit, in.Offset)
		log.Info(msg)
		return errors.New(msg)
	}

	res, err := db.DbAdapter.ListFileShare(ctx, int(in.Limit), int(in.Offset), in.Filter)
	if err != nil {
		log.Errorf("Failed to List FileShares: %s \n", err)
		return err
	}

	var fileshares []*pb.FileShare
	for _, fs := range res {
		fileshare := &pb.FileShare{
			Id:                 fs.Id.Hex(),
			CreatedAt:          fs.CreatedAt,
			UpdatedAt:          fs.UpdatedAt,
			Name:               fs.Name,
			Description:        fs.Description,
			TenantId:           fs.TenantId,
			UserId:             fs.UserId,
			BackendId:          fs.BackendId,
			Backend:            fs.Backend,
			Size:               *fs.Size,
			Type:               fs.Type,
			Status:             fs.Status,
			Region:             fs.Region,
			AvailabilityZone:   fs.AvailabilityZone,
			Protocols:          fs.Protocols,
			SnapshotId:         fs.SnapshotId,
			Encrypted:          *fs.Encrypted,
			EncryptionSettings: fs.EncryptionSettings,
		}

		if utils.UpdateFileShareStruct(fs, fileshare) != nil {
			log.Errorf("Failed to update fileshare struct: %v , %s\n", fs, err)
			return err
		}

		fileshares = append(fileshares, fileshare)
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
		log.Errorf("Failed to get fileshare: %s\n", err)
		return err
	}

	fileshare := &pb.FileShare{
		Id:                 fs.Id.Hex(),
		CreatedAt:          fs.CreatedAt,
		UpdatedAt:          fs.UpdatedAt,
		Name:               fs.Name,
		Description:        fs.Description,
		TenantId:           fs.TenantId,
		UserId:             fs.UserId,
		BackendId:          fs.BackendId,
		Backend:            fs.Backend,
		Size:               *fs.Size,
		Type:               fs.Type,
		Status:             fs.Status,
		Region:             fs.Region,
		AvailabilityZone:   fs.AvailabilityZone,
		Protocols:          fs.Protocols,
		SnapshotId:         fs.SnapshotId,
		Encrypted:          *fs.Encrypted,
		EncryptionSettings: fs.EncryptionSettings,
	}

	if utils.UpdateFileShareStruct(fs, fileshare) != nil {
		log.Errorf("Failed to update fileshare struct: %+v , %s\n", fs, err)
		return err
	}
	log.Debugf("Get File share response, fileshare: %+v \n", fileshare)

	out.Fileshare = fileshare

	log.Info("Got file share successfully.")
	return nil
}

func (f *fileService) CreateFileShare(ctx context.Context, in *pb.CreateFileShareRequest, out *pb.CreateFileShareResponse) error {
	log.Info("Received CreateFileShare request.")

	backend, err := utils.GetBackend(ctx, f.backendClient, in.Fileshare.BackendId)
	if err != nil {
		log.Errorln("Failed to get backend client with err:", err)
		return err
	}

	sd, err := driver.CreateStorageDriver(backend.Backend)
	if err != nil {
		log.Errorln("Failed to create Storage driver err:", err)
		return err
	}

	fs, err := sd.CreateFileShare(ctx, in)
	if err != nil {
		log.Errorf("Received error in creating file shares at backend %s", err)
		if fs == nil {
			return err
		}
		fs.Fileshare.Status = utils.FileShareStateError
	} else {
		fs.Fileshare.Status = utils.FileShareStateCreating
	}

	fileshare := &model.FileShare{
		Name:               in.Fileshare.Name,
		Description:        in.Fileshare.Description,
		TenantId:           in.Fileshare.TenantId,
		UserId:             in.Fileshare.UserId,
		BackendId:          in.Fileshare.BackendId,
		Backend:            backend.Backend.Name,
		CreatedAt:          time.Now().Format(utils.TimeFormat),
		UpdatedAt:          time.Now().Format(utils.TimeFormat),
		Type:               in.Fileshare.Type,
		Status:             fs.Fileshare.Status,
		Region:             backend.Backend.Region,
		AvailabilityZone:   in.Fileshare.AvailabilityZone,
		Protocols:          in.Fileshare.Protocols,
		SnapshotId:         in.Fileshare.SnapshotId,
		Size:               &fs.Fileshare.Size,
		Encrypted:          &fs.Fileshare.Encrypted,
		EncryptionSettings: fs.Fileshare.EncryptionSettings,
	}

	if utils.UpdateFileShareModel(fs.Fileshare, fileshare) != nil {
		log.Errorf("Failed to update fileshare model: %+v , %s\n", fileshare, err)
		return err
	}

	if in.Fileshare.Id != "" {
		fileshare.Id = bson.ObjectIdHex(in.Fileshare.Id)
	}
	log.Debugf("Create File Share Model: %+v", fileshare)

	res, err := db.DbAdapter.CreateFileShare(ctx, fileshare)
	if err != nil {
		log.Errorf("Failed to create file share: %+v in db", err)
		return err
	}

	metadataFS, err := driverutils.ConvertMapToStruct(fileshare.Metadata)
	if err != nil {
		log.Error(err)
		return err
	}

	out.Fileshare = &pb.FileShare{
		Id:                 res.Id.Hex(),
		CreatedAt:          res.CreatedAt,
		UpdatedAt:          res.UpdatedAt,
		Name:               res.Name,
		Description:        res.Description,
		TenantId:           res.TenantId,
		UserId:             res.UserId,
		BackendId:          res.BackendId,
		Backend:            res.Backend,
		Size:               *res.Size,
		Type:               res.Type,
		Status:             res.Status,
		Region:             res.Region,
		AvailabilityZone:   res.AvailabilityZone,
		Tags:               in.Fileshare.Tags,
		Protocols:          res.Protocols,
		SnapshotId:         res.SnapshotId,
		Encrypted:          *res.Encrypted,
		EncryptionSettings: res.EncryptionSettings,
		Metadata:           metadataFS,
	}
	log.Debugf("Create File share response, fileshare: %+v\n", out.Fileshare)

	log.Info("Created file share successfully.")

	// SyncFileShare to Update the FileShare DB Model based on the Backend Resource in the background
	go f.SyncFileShare(ctx, out.Fileshare, backend.Backend)
	return nil
}

func (f *fileService) UpdateFileShare(ctx context.Context, in *pb.UpdateFileShareRequest,
	out *pb.UpdateFileShareResponse) error {

	log.Info("Received UpdateFileShare request.")

	res, err := db.DbAdapter.GetFileShare(ctx, in.Id)
	if err != nil {
		log.Errorf("Failed to get fileshare: [%v] from db %s\n", res, err)
		return err
	}

	backend, err := utils.GetBackend(ctx, f.backendClient, res.BackendId)
	if err != nil {
		log.Errorln("Failed to get backend client with err: ", err)
		return err
	}

	sd, err := driver.CreateStorageDriver(backend.Backend)
	if err != nil {
		log.Errorln("Failed to create Storage driver err: ", err)
		return err
	}

	in.Fileshare.Name = res.Name
	in.Fileshare.AvailabilityZone = res.AvailabilityZone
	if backend.Backend.Type == constants.BackendTypeAwsFile {
		metaMap, err := utils.ConvertMetadataStructToMap(in.Fileshare.Metadata)
		if err != nil {
			log.Errorf("Failed to convert metaStruct: [%+v] to metaMap %s", in.Fileshare.Metadata, err)
			return err
		}
		metaMap[aws.FileSystemId] = res.Metadata[aws.FileSystemId]
		metaStruct, err := driverutils.ConvertMapToStruct(metaMap)
		if err != nil {
			log.Errorf("Failed to convert metaMap: [%+v] to metaStruct %s\n", metaMap, err)
			return err
		}
		in.Fileshare.Metadata = metaStruct
	} else if backend.Backend.Type == constants.BackendTypeGcsFile || backend.Backend.Type == constants.BackendTypeHwSFS {
		metaStruct, err := driverutils.ConvertMapToStruct(res.Metadata)
		if err != nil {
			log.Errorf("Failed to convert metaMap: [%+v] to metaStruct %s\n", res.Metadata, err)
			return err
		}
		in.Fileshare.Metadata = metaStruct
	}

	fs, err := sd.UpdatefileShare(ctx, in)
	if err != nil {
		log.Errorf("received error in updating file shares at backend %s", err)
		if fs == nil {
			return err
		}
		fs.Fileshare.Status = utils.FileShareStateError
	} else {
		fs.Fileshare.Status = utils.FileShareStateUpdating
	}

	fileshare := &model.FileShare{
		Id:                 res.Id,
		Name:               res.Name,
		TenantId:           res.TenantId,
		UserId:             res.UserId,
		BackendId:          res.BackendId,
		Backend:            backend.Backend.Name,
		CreatedAt:          res.CreatedAt,
		UpdatedAt:          time.Now().Format(utils.TimeFormat),
		Type:               res.Type,
		Status:             fs.Fileshare.Status,
		Region:             res.Region,
		AvailabilityZone:   res.AvailabilityZone,
		Protocols:          res.Protocols,
		SnapshotId:         res.SnapshotId,
		Size:               &fs.Fileshare.Size,
		Encrypted:          res.Encrypted,
		EncryptionSettings: res.EncryptionSettings,
		Tags:               res.Tags,
		Metadata:           res.Metadata,
	}

	if in.Fileshare.Description != "" {
		fileshare.Description = in.Fileshare.Description
	}

	if utils.UpdateFileShareModel(fs.Fileshare, fileshare) != nil {
		log.Errorf("Failed to update fileshare model: %+v, %s\n", fileshare, err)
		return err
	}
	log.Debugf("Update File Share Model: %+v", fileshare)

	upateRes, err := db.DbAdapter.UpdateFileShare(ctx, fileshare)
	if err != nil {
		log.Errorf("Failed to update file share: %+v, %s", fileshare, err)
		return err
	}

	out.Fileshare = &pb.FileShare{
		Id:                 upateRes.Id.Hex(),
		CreatedAt:          upateRes.CreatedAt,
		UpdatedAt:          upateRes.UpdatedAt,
		Name:               upateRes.Name,
		Description:        upateRes.Description,
		TenantId:           upateRes.TenantId,
		UserId:             upateRes.UserId,
		BackendId:          upateRes.BackendId,
		Backend:            upateRes.Backend,
		Size:               *upateRes.Size,
		Type:               upateRes.Type,
		Status:             upateRes.Status,
		Region:             upateRes.Region,
		AvailabilityZone:   upateRes.AvailabilityZone,
		Protocols:          res.Protocols,
		SnapshotId:         upateRes.SnapshotId,
		Encrypted:          *upateRes.Encrypted,
		EncryptionSettings: upateRes.EncryptionSettings,
	}

	if utils.UpdateFileShareStruct(upateRes, out.Fileshare) != nil {
		log.Errorf("Failed to update fileshare struct: %v, %s\n", fs, err)
		return err
	}
	log.Debugf("Update File share response, fileshare: %+v\n", out.Fileshare)

	log.Info("Updated file share successfully.")

	// SyncFileShare to Update the FileShare DB Model based on the Backend Resource in the background
	go f.SyncFileShare(ctx, out.Fileshare, backend.Backend)
	return nil
}

func (f *fileService) DeleteFileShare(ctx context.Context, in *pb.DeleteFileShareRequest, out *pb.DeleteFileShareResponse) error {

	log.Info("Received DeleteFileShare request.")

	res, err := db.DbAdapter.GetFileShare(ctx, in.Id)
	if err != nil {
		log.Errorf("Failed to get fileshare: [%v] from db %s\n", res, err)
		return err
	}

	backend, err := utils.GetBackend(ctx, f.backendClient, res.BackendId)
	if err != nil {
		log.Errorln("Failed to get backend client with err:", err)
		return err
	}

	sd, err := driver.CreateStorageDriver(backend.Backend)
	if err != nil {
		log.Errorln("Failed to create Storage driver err:", err)
		return err
	}

	fileshare := &pb.FileShare{
		Name:             res.Name,
		AvailabilityZone: res.AvailabilityZone,
	}

	metadata, err := driverutils.ConvertMapToStruct(res.Metadata)
	if err != nil {
		log.Error(err)
		return err
	}
	fileshare.Metadata = metadata

	in.Fileshare = fileshare

	_, err = sd.DeleteFileShare(ctx, in)
	if err != nil {
		log.Errorf("Received error in deleting file shares at backend %s ", err)
		return err
	}

	err = db.DbAdapter.DeleteFileShare(ctx, in.Id)
	if err != nil {
		log.Errorf("Failed to delete file share err: %s \n", err)
		return err
	}
	log.Info("Deleted file share successfully.")
	return nil
}

func (f *fileService) SyncFileShare(ctx context.Context, fs *pb.FileShare, backend *backend.BackendDetail) {
	log.Info("Received SyncFileShare request.")

	sd, err := driver.CreateStorageDriver(backend)
	if err != nil {
		log.Errorln("Failed to create Storage driver err:", err)
		return
	}

	time.Sleep(6 * time.Second)

	ctxBg := context.Background()
	ctxBg, _ = context.WithTimeout(ctxBg, 10*time.Minute)

	fsGetInput := &pb.GetFileShareRequest{Fileshare: fs}

	getFs, err := sd.GetFileShare(ctxBg, fsGetInput)
	if err != nil {
		fs.Status = utils.FileShareStateError
		log.Errorf("Received error in getting file shares at backend %s", err)
		return
	}
	log.Debugf("Get File share response = [%+v] from backend", getFs)

	if utils.MergeFileShareData(getFs.Fileshare, fs) != nil {
		log.Errorf("Failed to merge file share create data: [%+v] and get data: [%+v]\n", fs, err)
		return
	}

	fileshare := &model.FileShare{
		Id:                 bson.ObjectIdHex(fs.Id),
		Name:               fs.Name,
		Description:        fs.Description,
		CreatedAt:          fs.CreatedAt,
		UpdatedAt:          time.Now().Format(utils.TimeFormat),
		TenantId:           fs.TenantId,
		UserId:             fs.UserId,
		BackendId:          fs.BackendId,
		Backend:            fs.Backend,
		Type:               fs.Type,
		Size:               &fs.Size,
		Region:             fs.Region,
		AvailabilityZone:   fs.AvailabilityZone,
		Status:             fs.Status,
		Protocols:          fs.Protocols,
		SnapshotId:         fs.SnapshotId,
		Encrypted:          &fs.Encrypted,
		EncryptionSettings: fs.EncryptionSettings,
	}

	if utils.UpdateFileShareModel(fs, fileshare) != nil {
		log.Errorf("Failed to update fileshare model: %+v , %s\n", fileshare, err)
		return
	}

	res, err := db.DbAdapter.UpdateFileShare(ctx, fileshare)
	if err != nil {
		log.Errorf("Failed to update file share: %+v in db", err)
		return
	}

	log.Debugf("Sync file share: [%+v] to db successfully.", res)
	return
}
