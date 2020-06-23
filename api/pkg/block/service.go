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

package block

import (
    "net/http"
	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/v2/client"
	"github.com/opensds/multi-cloud/api/pkg/common"
	"github.com/opensds/multi-cloud/api/pkg/policy"
	"github.com/opensds/multi-cloud/backend/proto"
	"github.com/opensds/multi-cloud/block/proto"
    c "github.com/opensds/multi-cloud/api/pkg/context"
    log "github.com/sirupsen/logrus"
)

const (
	blockService   = "block"
	backendService = "backend"
)

type APIService struct {
	blockClient   block.BlockService
	backendClient backend.BackendService
}

func NewAPIService(c client.Client) *APIService {
	return &APIService{
		blockClient:   block.NewBlockService(blockService, c),
		backendClient: backend.NewBackendService(backendService, c),
	}
}

// Create Volume 
func (s *APIService) CreateVolume(request *restful.Request, response *restful.Response) {
    if !policy.Authorize(request, response, "volume:create") {
        return
    }
    log.Info("Received request to create volume.")

    volumeDetail := &block.Volume{}
    err := request.ReadEntity(&volumeDetail)
    if err != nil {
        log.Errorf("failed to read request body: %v\n", err)
        response.WriteError(http.StatusInternalServerError, err)
        return
    }

    ctx := common.InitCtxWithAuthInfo(request)
    actx := request.Attribute(c.KContext).(*c.Context)
    volumeDetail.TenantId = actx.TenantId
    volumeDetail.UserId = actx.UserId

    res, err := s.blockClient.CreateVolume(ctx, &block.CreateVolumeRequest{Volume: volumeDetail})
    if err != nil {
        log.Errorf("Failed to create volume: %v\n", err)
        response.WriteError(http.StatusInternalServerError, err)
        return
    }

    log.Info("Create Volume successfully.")
    response.WriteEntity(res.Volume)
}

// List all volumes
func (s *APIService) ListVolumes(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "volume:list") {
		return
	}
	log.Infof("Received request to list volumes")

	backend := request.QueryParameter(common.REQUEST_PATH_BACKEND_ID)
	ctx := common.InitCtxWithAuthInfo(request)
	rsp, err := s.blockClient.ListVolumes(ctx, &block.VolumeRequest{BackendId: backend})
	log.Infof("Volume resp is rsp =[%v]\n", rsp)
	if err != nil {
		log.Errorf("List volumes failed with error=%v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	// write response
	log.Infoln("Listed Volumes successfully\n")
	response.WriteEntity(rsp)
}

