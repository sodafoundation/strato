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
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	"github.com/opensds/multi-cloud/block/pkg/datastore/driver"
	"github.com/opensds/multi-cloud/block/pkg/model"
	pb "github.com/opensds/multi-cloud/block/proto"
	log "github.com/sirupsen/logrus"
)

func (b *blockService) getAccessInfo(ctx context.Context, BackendId string) (*backendpb.BackendDetail, error){
	// get the backedn access_info
	backendResp, backendErr := b.backendClient.GetBackend(ctx, &backendpb.GetBackendRequest{
		Id: BackendId})

	if backendErr != nil {
		log.Infof("Get backend failed with error: %v\n.", backendErr)
		return nil, backendErr
	}

	accessInfo := &backendpb.BackendDetail{
		Region:   backendResp.Backend.Region,
		Access:   backendResp.Backend.Access,
		Security: backendResp.Backend.Security,
	}

    return accessInfo, nil
}

func (b *blockService) ListVolumes(ctx context.Context, in *pb.VolumeRequest, out *pb.ListVolumesResponse) error {
	log.Infof("ListVolumes called in block service for backend")

    accessInfo, accessErr := b.getAccessInfo(ctx, in.BackendId)
    if accessErr != nil {
        log.Errorf("Failed to get the Access info for the backend:", accessErr)
        return accessErr
    }

	sd, err := driver.CreateStorageDriver("aws-block", accessInfo)
	if err != nil {
		log.Errorln("Failed to create Storage driver err:", err)
		return err
	}
	volResp, err := sd.List(ctx)
	if err != nil {
		log.Errorf("Received error in getting volumes ", err)
		return err
	}
	// TODO: paging list
	out.Volumes = volResp.Volumes

	log.Infof("List of Volumes:%+v\n", out.Volumes)
	return nil
}

func (b *blockService) CreateVolume(ctx context.Context, in *pb.CreateVolumeRequest, out *pb.CreateVolumeResponse) error {
	log.Infof("Create volume called in block service.")
    volume := &model.Volume{
        Name:               in.Volume.Name,
        TenantId:           in.Volume.TenantId,
        UserId:             in.Volume.UserId,
        Type:               in.Volume.Type,
        AvailabilityZone:   in.Volume.AvailabilityZone,
        BackendId:          in.Volume.BackendId,
        Size:               in.Volume.Size,
        Iops:               in.Volume.Iops,
    }

    // Get the require access details
    accessInfo, accessErr := b.getAccessInfo(ctx, in.Volume.BackendId)
    if accessErr != nil {
        log.Errorf("Failed to get the Access info for the backend:", accessErr)
        return accessErr
    }

    // Create the specific driver. Here it is AWS
	sd, err := driver.CreateStorageDriver("aws-block", accessInfo)
	if err != nil {
		log.Errorln("Failed to create Storage driver err:", err)
		return err
	}

    // Create the volume
	volResp, err := sd.Create(ctx, volume)
	if err != nil {
		log.Errorf("Received error in creating volumes ", err)
		return err
	}

    out.Volume = volResp.Volume
    log.Infof("Created volume is:%+v\n", out.Volume)
    return nil
}


