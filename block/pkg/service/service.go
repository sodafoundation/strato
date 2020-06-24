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

package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/opensds/multi-cloud/contrib/datastore/block/aws"
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
		log.Errorf("Failed to List Volumes: \n", err)
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
			log.Errorf("Failed to update volume struct: %v\n", vol, err)
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
		log.Errorf("Failed to get volume: \n", err)
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
		log.Errorf("Failed to update volume struct: %+v\n", vol, err)
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
		log.Errorf("Received error in creating volume at backend ", err)
		vol.Volume.Status = utils.VolumeStateError
	} else {
		if backend.Backend.Type == constants.BackendTypeAwsBlock {
			time.Sleep(4 * time.Second)
		}

		volGetInput := &pb.GetVolumeRequest{Volume: vol.Volume}

		getVol, err := sd.GetVolume(ctx, volGetInput)
		if err != nil {
			vol.Volume.Status = utils.VolumeStateError
			log.Errorf("Received error in getting volume at backend ", err)
			return err
		}
		log.Debugf("Get Volumee response = [%+v] from backend", getVol)

		if utils.MergeVolumeData(getVol.Volume, vol.Volume) != nil {
			log.Errorf("Failed to merge volume create data: [%+v] and get data: [%+v] %v\n", vol, err)
			return err
		}
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
		Region:             in.Volume.Region,
		AvailabilityZone:   in.Volume.AvailabilityZone,
		SnapshotId:         in.Volume.SnapshotId,
		Size:               &vol.Volume.Size,
		Encrypted:          &vol.Volume.Encrypted,
		EncryptionSettings: vol.Volume.EncryptionSettings,
	}

	if utils.UpdateVolumeModel(vol.Volume, volume) != nil {
		log.Errorf("Failed to update volume model: %+v\n", volume, err)
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
	return nil
}

func (f *blockService) UpdateVolume(ctx context.Context, in *pb.UpdateVolumeRequest,
	out *pb.UpdateVolumeResponse) error {

	log.Info("Received UpdateVolume request.")

	res, err := db.DbAdapter.GetVolume(ctx, in.Id)
	if err != nil {
		log.Errorf("Failed to get volume: [%v] from db\n", res, err)
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

	in.Volume.Name = res.Name
	if backend.Backend.Type == constants.BackendTypeAwsBlock {
		metaMap, err := utils.ConvertMetadataStructToMap(in.Volume.Metadata)
		if err != nil {
			log.Errorf("Failed to convert metaStruct: [%+v] to metaMap", in.Volume.Metadata, err)
			return err
		}
		metaMap[aws.VolumeId] = res.Metadata[aws.VolumeId]
		metaStruct, err := driverutils.ConvertMapToStruct(metaMap)
		if err != nil {
			log.Errorf("Failed to convert metaMap: [%+v] to metaStruct\n", metaMap, err)
			return err
		}
		in.Volume.Metadata = metaStruct
	}

	vol, err := sd.UpdateVolume(ctx, in)
	if err != nil {
		log.Errorf("Received error in updating volumes at backend ", err)
		vol.Volume.Status = utils.VolumeStateError
	} else {
		if backend.Backend.Type == constants.BackendTypeAwsBlock {
			time.Sleep(4 * time.Second)
		}

		volGetInput := &pb.GetVolumeRequest{Volume: vol.Volume}

		getVol, err := sd.GetVolume(ctx, volGetInput)
		if err != nil {
			log.Errorf("Received error in getting volume at backend ", err)
			return err
		}
		log.Debugf("Get Volume response= [%+v] from backend", getVol)

		if utils.MergeVolumeData(getVol.Volume, vol.Volume) != nil {
			log.Errorf("Failed to merge volume create data: [%+v] and get data: [%+v] %v\n", vol, err)
			return err
		}
	}

	volume := &model.Volume{
		Id:                 res.Id,
		Name:               res.Name,
		Description:        in.Volume.Description,
		TenantId:           res.TenantId,
		UserId:             res.UserId,
		BackendId:          res.BackendId,
		Backend:            backend.Backend.Name,
		CreatedAt:          res.CreatedAt,
		UpdatedAt:          time.Now().Format(utils.TimeFormat),
		Type:               res.Type,
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
		log.Errorf("Failed to update volume model: %+v\n", volume, err)
		return err
	}
	log.Debugf("Update Volume Model: %+v", volume)

	upateRes, err := db.DbAdapter.UpdateVolume(ctx, volume)
	if err != nil {
		log.Errorf("Failed to update volume: %+v", volume, err)
		return err
	}

	out.Volume = &pb.Volume{
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
		SnapshotId:         upateRes.SnapshotId,
		Encrypted:          *upateRes.Encrypted,
		EncryptionSettings: upateRes.EncryptionSettings,
	}

	if utils.UpdateVolumeStruct(upateRes, out.Volume) != nil {
		log.Errorf("Failed to update volume struct: %v\n", upateRes, err)
		return err
	}
	log.Debugf("Update Volume response, volume: %+v\n", out.Volume)

	log.Info("Updated Volume successfully.")
	return nil
}

func (b *blockService) DeleteVolume(ctx context.Context, in *pb.DeleteVolumeRequest, out *pb.DeleteVolumeResponse) error {

	log.Info("Received DeleteVolume request.")
/*
	res, err := db.DbAdapter.GetVolume(ctx, in.Id)
	if err != nil {
		log.Errorf("Failed to get volume: [%v] from db\n", res, err)
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
		log.Errorf("Received error in deleting volume at backend ", err)
		return err
	}
*/
	err := db.DbAdapter.DeleteVolume(ctx, in.Id)
	if err != nil {
		log.Errorf("Failed to delete volume err:\n", err)
		return err
	}
	log.Info("Deleted Volume successfully.")
	return nil
}
