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
	"github.com/opensds/multi-cloud/contrib/datastore/block/common"
	"time"

	"github.com/globalsign/mgo/bson"
	"github.com/micro/go-micro/v2/client"
	"github.com/opensds/multi-cloud/backend/pkg/utils/constants"
	"github.com/opensds/multi-cloud/block/pkg/db"
	"github.com/opensds/multi-cloud/block/pkg/model"
	"github.com/opensds/multi-cloud/block/pkg/utils"
	"github.com/opensds/multi-cloud/contrib/datastore/block/driver"

	backend "github.com/opensds/multi-cloud/backend/proto"
	pb "github.com/opensds/multi-cloud/block/proto"
	driverutils "github.com/opensds/multi-cloud/contrib/utils"
	log "github.com/sirupsen/logrus"
)

type blockService struct {
	blockClient   pb.BlockService
	backendClient backend.BackendService
}

func NewBlockService() pb.BlockHandler {

	log.Infof("Init block service finished.\n")
	return &blockService{
		blockClient:   pb.NewBlockService("block", client.DefaultClient),
		backendClient: backend.NewBackendService("backend", client.DefaultClient),
	}
}

func (b *blockService) ListVolume(ctx context.Context, in *pb.ListVolumeRequest, out *pb.ListVolumeResponse) error {
	log.Info("Received ListVolume request.")

	if in.Limit < 0 || in.Offset < 0 {
		msg := fmt.Sprintf("Invalid pagination parameter, limit = %d and offset = %d.", in.Limit, in.Offset)
		log.Info(msg)
		return errors.New(msg)
	}

	res, err := db.DbAdapter.ListVolume(ctx, int(in.Limit), int(in.Offset), in.Filter)
	if err != nil {
		log.Errorf("Failed to List Volumes, error: %s\n", err)
		return err
	}

	var volumes []*pb.Volume
	for _, vol := range res {
		volume := &pb.Volume{
			Id:                 vol.Id.Hex(),
			CreatedAt:          vol.CreatedAt,
			UpdatedAt:          vol.UpdatedAt,
			Name:               vol.Name,
			Description:        vol.Description,
			TenantId:           vol.TenantId,
			UserId:             vol.UserId,
			BackendId:          vol.BackendId,
			Backend:            vol.Backend,
			Size:               *vol.Size,
			Type:               vol.Type,
			Status:             vol.Status,
			Region:             vol.Region,
			AvailabilityZone:   vol.AvailabilityZone,
			Iops:               vol.Iops,
			MultiAttachEnabled: vol.MultiAttach,
			SnapshotId:         vol.SnapshotId,
			Encrypted:          *vol.Encrypted,
			EncryptionSettings: vol.EncryptionSettings,
		}

		if utils.UpdateVolumeStruct(vol, volume) != nil {
			log.Errorf("Failed to update volume struct: %v , error: %s\n", vol, err)
			return err
		}

		volumes = append(volumes, volume)
	}
	out.Volumes = volumes
	out.Next = in.Offset + int32(len(res))

	log.Debugf("Listed Volume successfully, #num=%d, volumes: %+v\n", len(volumes), volumes)

	return nil
}

func (b *blockService) GetVolume(ctx context.Context, in *pb.GetVolumeRequest, out *pb.GetVolumeResponse) error {
	log.Info("Received GetVolume request.")

	vol, err := db.DbAdapter.GetVolume(ctx, in.Id)
	if err != nil {
		log.Errorf("Failed to get volume: , error: %s\n", err)
		return err
	}

	volume := &pb.Volume{
		Id:                 vol.Id.Hex(),
		CreatedAt:          vol.CreatedAt,
		UpdatedAt:          vol.UpdatedAt,
		Name:               vol.Name,
		Description:        vol.Description,
		TenantId:           vol.TenantId,
		UserId:             vol.UserId,
		BackendId:          vol.BackendId,
		Backend:            vol.Backend,
		Size:               *vol.Size,
		Type:               vol.Type,
		Status:             vol.Status,
		Region:             vol.Region,
		AvailabilityZone:   vol.AvailabilityZone,
		Iops:               vol.Iops,
		MultiAttachEnabled: vol.MultiAttach,
		SnapshotId:         vol.SnapshotId,
		Encrypted:          *vol.Encrypted,
		EncryptionSettings: vol.EncryptionSettings,
	}

	if utils.UpdateVolumeStruct(vol, volume) != nil {
		log.Errorf("Failed to update volume struct: %+v , error: %s\n", vol, err)
		return err
	}
	log.Debugf("Get Volume response, volume: %+v\n", volume)

	out.Volume = volume

	log.Info("Got Volume successfully.")
	return nil
}

func (b *blockService) CreateVolume(ctx context.Context, in *pb.CreateVolumeRequest, out *pb.CreateVolumeResponse) error {
	log.Info("Received CreateVolume request.")

	backend, err := utils.GetBackend(ctx, b.backendClient, in.Volume.BackendId)
	if err != nil {
		log.Errorln("Failed to get backend client with err:", err)
		return err
	}

	sd, err := driver.CreateStorageDriver(backend.Backend)
	if err != nil {
		log.Errorln("Failed to create Storage driver err:", err)
		return err
	}

	vol, err := sd.CreateVolume(ctx, in)
	if err != nil {
		log.Errorf("received error in creating volume at backend , error: %s ", err)
		if vol == nil {
			return err
		}
		vol.Volume.Status = utils.VolumeStateError
	} else {
		vol.Volume.Status = utils.VolumeStateCreating
	}

	volume := &model.Volume{
		Name:               in.Volume.Name,
		Description:        in.Volume.Description,
		TenantId:           in.Volume.TenantId,
		UserId:             in.Volume.UserId,
		BackendId:          in.Volume.BackendId,
		Backend:            backend.Backend.Name,
		CreatedAt:          time.Now().Format(utils.TimeFormat),
		UpdatedAt:          time.Now().Format(utils.TimeFormat),
		Type:               in.Volume.Type,
		Status:             vol.Volume.Status,
		Iops:               vol.Volume.Iops,
		Region:             backend.Backend.Region,
		AvailabilityZone:   in.Volume.AvailabilityZone,
		SnapshotId:         in.Volume.SnapshotId,
		Size:               &vol.Volume.Size,
		Encrypted:          &vol.Volume.Encrypted,
		EncryptionSettings: vol.Volume.EncryptionSettings,
	}

	if utils.UpdateVolumeModel(vol.Volume, volume) != nil {
		log.Errorf("Failed to update volume model: %+v , error: %s\n", volume, err)
		return err
	}

	if in.Volume.Id != "" {
		volume.Id = bson.ObjectIdHex(in.Volume.Id)
	}
	log.Debugf("Create Volume Model: %+v", volume)

	res, err := db.DbAdapter.CreateVolume(ctx, volume)
	if err != nil {
		log.Errorf("Failed to create volume: %+v in db", err)
		return err
	}

	metadataVol, err := driverutils.ConvertMapToStruct(volume.Metadata)
	if err != nil {
		log.Error(err)
		return err
	}

	out.Volume = &pb.Volume{
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
		Tags:               in.Volume.Tags,
		Iops:               res.Iops,
		SnapshotId:         res.SnapshotId,
		Encrypted:          *res.Encrypted,
		EncryptionSettings: res.EncryptionSettings,
		Metadata:           metadataVol,
	}
	log.Debugf("Create Volume response, volume: %+v\n", out.Volume)

	log.Info("Created volume successfully.")

	go b.SyncVolume(ctx, out.Volume, backend.Backend)
	return nil
}

func (b *blockService) UpdateVolume(ctx context.Context, in *pb.UpdateVolumeRequest,
	out *pb.UpdateVolumeResponse) error {

	log.Info("Received UpdateVolume request.")

	res, err := db.DbAdapter.GetVolume(ctx, in.Id)
	if err != nil {
		log.Errorf("Failed to get volume: [%v] from db , error: %s\n", res, err)
		return err
	}

	backend, err := utils.GetBackend(ctx, b.backendClient, res.BackendId)
	if err != nil {
		log.Errorln("Failed to get backend client with err:", err)
		return err
	}

	sd, err := driver.CreateStorageDriver(backend.Backend)
	if err != nil {
		log.Errorln("Failed to create Storage driver err:", err)
		return err
	}

	updatedName := res.Name
	if in.Volume.Name != "" {
		updatedName = in.Volume.Name
	}
	if backend.Backend.Type == constants.BackendTypeAwsBlock ||
		backend.Backend.Type == constants.BackendTypeHpcBlock {
		metaMap, err := utils.ConvertMetadataStructToMap(in.Volume.Metadata)
		if err != nil {
			log.Errorf("Failed to convert metaStruct: [%+v] to metaMap , error: %s", in.Volume.Metadata, err)
			return err
		}
		metaMap[common.VolumeId] = res.Metadata[common.VolumeId]
		metaStruct, err := driverutils.ConvertMapToStruct(metaMap)
		if err != nil {
			log.Errorf("Failed to convert metaMap: [%+v] to metaStruct, error: %s\n", metaMap, err)
			return err
		}
		in.Volume.Metadata = metaStruct
	}

	vol, err := sd.UpdateVolume(ctx, in)
	if err != nil {
		log.Errorf("received error in updating volumes at backend, error: %s ", err)
		if vol == nil {
			return err
		}
		vol.Volume.Status = utils.VolumeStateError
	} else {
		vol.Volume.Status = utils.VolumeStateUpdating
	}

	volume := &model.Volume{
		Id:                 res.Id,
		Name:               updatedName,
		Description:        in.Volume.Description,
		TenantId:           res.TenantId,
		UserId:             res.UserId,
		BackendId:          res.BackendId,
		Backend:            backend.Backend.Name,
		CreatedAt:          res.CreatedAt,
		UpdatedAt:          time.Now().Format(utils.TimeFormat),
		Type:               vol.Volume.Type,
		Status:             vol.Volume.Status,
		Iops:               vol.Volume.Iops,
		Region:             res.Region,
		AvailabilityZone:   res.AvailabilityZone,
		SnapshotId:         res.SnapshotId,
		Size:               &vol.Volume.Size,
		Encrypted:          res.Encrypted,
		EncryptionSettings: res.EncryptionSettings,
		Tags:               res.Tags,
		Metadata:           res.Metadata,
	}

	if utils.UpdateVolumeModel(vol.Volume, volume) != nil {
		log.Errorf("Failed to update volume model: %+v, error: %s\n", volume, err)
		return err
	}
	log.Debugf("Update Volume Model: %+v", volume)

	updateRes, err := db.DbAdapter.UpdateVolume(ctx, volume)
	if err != nil {
		log.Errorf("Failed to update volume: %+v, error: %s", volume, err)
		return err
	}

	out.Volume = &pb.Volume{
		Id:                 updateRes.Id.Hex(),
		CreatedAt:          updateRes.CreatedAt,
		UpdatedAt:          updateRes.UpdatedAt,
		Name:               updateRes.Name,
		Description:        updateRes.Description,
		TenantId:           updateRes.TenantId,
		UserId:             updateRes.UserId,
		BackendId:          updateRes.BackendId,
		Backend:            updateRes.Backend,
		Size:               *updateRes.Size,
		Type:               updateRes.Type,
		Iops:               updateRes.Iops,
		Status:             updateRes.Status,
		Region:             updateRes.Region,
		AvailabilityZone:   updateRes.AvailabilityZone,
		SnapshotId:         updateRes.SnapshotId,
		Encrypted:          *updateRes.Encrypted,
		EncryptionSettings: updateRes.EncryptionSettings,
	}

	if utils.UpdateVolumeStruct(updateRes, out.Volume) != nil {
		log.Errorf("Failed to update volume struct: %v, error: %s\n", updateRes, err)
		return err
	}
	log.Debugf("Update Volume response, volume: %+v\n", out.Volume)

	log.Info("Updated Volume successfully.")

	go b.SyncVolume(ctx, out.Volume, backend.Backend)
	return nil
}

func (b *blockService) DeleteVolume(ctx context.Context, in *pb.DeleteVolumeRequest, out *pb.DeleteVolumeResponse) error {

	log.Info("Received DeleteVolume request.")

	res, err := db.DbAdapter.GetVolume(ctx, in.Id)
	if err != nil {
		log.Errorf("Failed to get volume: [%v] from db, error: %s\n", res, err)
		return err
	}

	backend, err := utils.GetBackend(ctx, b.backendClient, res.BackendId)
	if err != nil {
		log.Errorln("Failed to get backend client with err:", err)
		return err
	}

	sd, err := driver.CreateStorageDriver(backend.Backend)
	if err != nil {
		log.Errorln("Failed to create Storage driver err:", err)
		return err
	}

	volume := &pb.Volume{
		Name: res.Name,
	}

	metadata, err := driverutils.ConvertMapToStruct(res.Metadata)
	if err != nil {
		log.Error(err)
		return err
	}
	volume.Metadata = metadata

	in.Volume = volume

	_, err = sd.DeleteVolume(ctx, in)
	if err != nil {
		log.Errorf("Received error in deleting volume at backend , error: %s", err)
		return err
	}

	err = db.DbAdapter.DeleteVolume(ctx, in.Id)
	if err != nil {
		log.Errorf("Failed to delete volume err:%s\n", err)
		return err
	}
	log.Info("Deleted volume successfully.")
	return nil
}

func (b *blockService) SyncVolume(ctx context.Context, vol *pb.Volume, backend *backend.BackendDetail) {
	log.Info("Received SyncVolume request.")

	sd, err := driver.CreateStorageDriver(backend)
	if err != nil {
		log.Errorln("Failed to create Storage driver err:", err)
		return
	}

	time.Sleep(6 * time.Second)

	ctxBg := context.Background()
	ctxBg, _ = context.WithTimeout(ctxBg, 10*time.Second)

	volGetInput := &pb.GetVolumeRequest{Volume: vol}

	getVol, err := sd.GetVolume(ctxBg, volGetInput)
	if err != nil {
		vol.Status = utils.VolumeStateError
		log.Errorf("Received error in getting volume at backend, error: %s ", err)
		return
	}
	log.Debugf("Get Volume response = [%+v] from backend", getVol)

	if utils.MergeVolumeData(getVol.Volume, vol) != nil {
		log.Errorf("Failed to merge volume create data: [%+v] and get data: [%+v]\n", vol, err)
		return
	}

	volume := &model.Volume{
		Id:                 bson.ObjectIdHex(vol.Id),
		Name:               vol.Name,
		Description:        vol.Description,
		CreatedAt:          vol.CreatedAt,
		UpdatedAt:          time.Now().Format(utils.TimeFormat),
		TenantId:           vol.TenantId,
		UserId:             vol.UserId,
		BackendId:          vol.BackendId,
		Backend:            vol.Backend,
		Type:               vol.Type,
		Iops:               vol.Iops,
		Size:               &vol.Size,
		Region:             vol.Region,
		AvailabilityZone:   vol.AvailabilityZone,
		Status:             vol.Status,
		SnapshotId:         vol.SnapshotId,
		Encrypted:          &vol.Encrypted,
		EncryptionSettings: vol.EncryptionSettings,
	}

	if utils.UpdateVolumeModel(vol, volume) != nil {
		log.Errorf("Failed to update volume model: %+v, error: %s\n", vol, err)
		return
	}

	res, err := db.DbAdapter.UpdateVolume(ctx, volume)
	if err != nil {
		log.Errorf("Failed to update volume: %+v in db", err)
		return
	}

	log.Debugf("Sync volume: [%+v] to db successfully.", res)
	return
}
