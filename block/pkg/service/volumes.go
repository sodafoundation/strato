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
    "github.com/opensds/multi-cloud/block/pkg/datastore/driver"
	pb "github.com/opensds/multi-cloud/block/proto"
    backendpb "github.com/opensds/multi-cloud/backend/proto"
    log "github.com/sirupsen/logrus"
)


func (b *blockService) ListVolumes(ctx context.Context, in *pb.VolumeRequest, out *pb.ListVolumesResponse) error {
	log.Infof("ListVolumes called in block service.[%v]", in.BackendId)
	// get the access_info
	backendResp, backendErr := b.backendClient.GetBackend(ctx, &backendpb.GetBackendRequest{
                Id: in.BackendId})

    log.Infof("backendErr is %v:", backendErr)
    if backendErr != nil {
        log.Infof("Get backend failed with error: %v\n.", backendErr)
        return backendErr
    } else {
        log.Infof("backendRep=%+v\n",  backendResp)
    }

	accessInfo := &backendpb.BackendDetail{
				Region:     backendResp.Backend.Region,
				Access:     backendResp.Backend.Access,
                Security:   backendResp.Backend.Security,
        }

    sd, err := driver.CreateStorageDriver("aws-block", accessInfo)
	if err != nil {
	    log.Errorln("Failed to create Storage driver err:", err)
	    return err
    }
    volResp, err := sd.List(ctx)
    if err != nil {
        log.Errorf("received error in getting volumes ", err)
        return err
    }
	// TODO: paging list
	for _, vol := range volResp {
        out.Volumes = append(out.Volumes, &pb.Volume{
	                                Name:  vol.Name,
                                    VolId: vol.VolId,
                                    VolSize: vol.VolSize,
                                    VolType: vol.VolType,
                                    VolStatus: vol.VolStatus,
                                    VolMultiAttachEnabled: vol.VolMultiAttachEnabled,
                                    VolEncrypted: vol.VolEncrypted,

	                        })
	}

	log.Infof("List of Volumes:%+v\n", out.Volumes)
	return nil
}
