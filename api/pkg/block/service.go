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

package block

import (
	"context"
	"net/http"

	"github.com/opensds/multi-cloud/block/pkg/model"
	"github.com/opensds/multi-cloud/contrib/utils"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/v2/client"
	log "github.com/sirupsen/logrus"

	"github.com/opensds/multi-cloud/api/pkg/common"
	c "github.com/opensds/multi-cloud/api/pkg/context"
	"github.com/opensds/multi-cloud/api/pkg/policy"
	backend "github.com/opensds/multi-cloud/backend/proto"
	block "github.com/opensds/multi-cloud/block/proto"
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

func UpdateFilter(reqFilter map[string]string, filter map[string]string) error {
	log.Debugf("Getting the filters updated for the request.")

	for k, v := range filter {
		log.Debugf("Filter Key = [%+v], Key = [%+v]\n", k, v)
		reqFilter[k] = v
	}
	return nil
}

func WriteError(response *restful.Response, msg string, errCode int, err error) {
	log.Errorf(msg, err)
	error := response.WriteError(errCode, err)
	if error != nil {
		log.Errorf("Response write status and http error failed: %v\n", err)
		return
	}
}

func (s *APIService) checkBackendExists(ctx context.Context, request *restful.Request,
	response *restful.Response, backendId string) error {

	backendResp, err := s.backendClient.GetBackend(ctx, &backend.GetBackendRequest{Id: backendId})
	if err != nil {
		WriteError(response, "Get Backend details failed: %v\n", http.StatusNotFound, err)
		return err
	}
	log.Infof("Backend response = [%+v]\n", backendResp)

	return nil
}

func (s *APIService) prepareVolumeRequest(request *restful.Request,
	response *restful.Response) *block.ListVolumeRequest {

	limit, offset, err := common.GetPaginationParam(request)
	if err != nil {
		WriteError(response, "Get pagination parameters failed: %v\n", http.StatusInternalServerError, err)
		return nil
	}

	sortKeys, sortDirs, err := common.GetSortParam(request)
	if err != nil {
		WriteError(response, "Get sort parameters failed: %v\n", http.StatusInternalServerError, err)
		return nil
	}

	filterOpts := []string{"name", "type", "status"}
	filter, err := common.GetFilter(request, filterOpts)
	if err != nil {
		WriteError(response, "Get filter failed: %v\n", http.StatusInternalServerError, err)
		return nil
	}

	return &block.ListVolumeRequest{
		Limit:    limit,
		Offset:   offset,
		SortKeys: sortKeys,
		SortDirs: sortDirs,
		Filter:   filter,
	}
}

func (s *APIService) ListVolumes(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "volume:list") {
		return
	}
	log.Info("Received request for Volume List.")

	ctx := common.InitCtxWithAuthInfo(request)

	backendId := request.QueryParameter(common.REQUEST_PATH_BACKEND_ID)

	var listVolumeRequest *block.ListVolumeRequest

	if backendId != "" {
		//List volumes for the backend.
		listVolumeRequest = s.listVolumeByBackend(ctx, request, response, backendId)
	} else {
		//List all volumes.
		listVolumeRequest = s.listVolumeDefault(ctx, request, response)
	}

	if listVolumeRequest == nil {
		log.Errorf("List Volumes failed: %v\n", http.StatusBadRequest)
		return
	}

	res, err := s.blockClient.ListVolume(ctx, listVolumeRequest)
	if err != nil {
		WriteError(response, "List Volumes failed: %v\n", http.StatusInternalServerError, err)
		return
	}

	log.Infoln("List Volumes successfully.")

	err = response.WriteEntity(res)
	if err != nil {
		log.Errorf("Response write entity failed: %v\n", err)
		return
	}
}

func (s *APIService) listVolumeDefault(ctx context.Context, request *restful.Request,
	response *restful.Response) *block.ListVolumeRequest {

	return s.prepareVolumeRequest(request, response)
}

func (s *APIService) listVolumeByBackend(ctx context.Context, request *restful.Request, response *restful.Response,
	backendId string) *block.ListVolumeRequest {

	if s.checkBackendExists(ctx, request, response, backendId) != nil {
		return nil
	}

	filterQueryOpts := []string{"backendId"}
	filterQuery, err := common.GetFilter(request, filterQueryOpts)
	if err != nil {
		WriteError(response, "Get filter failed: %v\n", http.StatusBadRequest, err)
		return nil
	}

	listVolumeRequest := s.prepareVolumeRequest(request, response)

	if listVolumeRequest == nil {
		log.Errorf("List Volumes failed: %v\n", http.StatusBadRequest)
		return nil
	}

	if UpdateFilter(listVolumeRequest.Filter, filterQuery) != nil {
		log.Errorf("Update filter failed: %v\n", err)
		return nil
	}

	return listVolumeRequest
}

func (s *APIService) GetVolume(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "volume:get") {
		return
	}
	log.Infof("Received request volume details: %s\n", request.PathParameter("id"))

	id := request.PathParameter("id")

	ctx := common.InitCtxWithAuthInfo(request)

	res, err := s.blockClient.GetVolume(ctx, &block.GetVolumeRequest{Id: id})
	if err != nil {
		log.Errorf("Failed to get volume details err: \n", err)
		response.WriteError(http.StatusNotFound, err)
		return
	}

	log.Info("Get Volume details successfully.")
	response.WriteEntity(res.Volume)
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

func (s *APIService) UpdateVolume(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "volume:put") {
		return
	}
	log.Info("Received request for updating volume.")

	volume := &model.Volume{}
	err := request.ReadEntity(&volume)
	if err != nil {
		log.Errorf("Failed to read request body err: \n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	vol := &block.Volume{
		Description: volume.Description,
	}

	if volume.Metadata != nil || len(volume.Metadata) != 0 {
		metadata, err := utils.ConvertMapToStruct(volume.Metadata)
		if err != nil {
			log.Errorf("Failed to parse metadata: %v\n", volume.Metadata, err)
			response.WriteError(http.StatusInternalServerError, err)
			return
		}

		vol.Metadata = metadata
	}

	if volume.Tags != nil {
		var tags []*block.Tag
		for _, tag := range volume.Tags {
			tags = append(tags, &block.Tag{
				Key:   tag.Key,
				Value: tag.Value,
			})
		}
		vol.Tags = tags
	}

	if volume.Size != nil {
		vol.Size = *volume.Size
	}

	vol.Name = volume.Name
	if volume.Encrypted != nil && *volume.Encrypted {
		vol.Encrypted = *volume.Encrypted
		vol.EncryptionSettings = volume.EncryptionSettings
	}

	vol.Iops = volume.Iops
	vol.Type = volume.Type

	id := request.PathParameter("id")

	ctx := common.InitCtxWithAuthInfo(request)

	actx := request.Attribute(c.KContext).(*c.Context)
	vol.TenantId = actx.TenantId
	vol.UserId = actx.UserId

	res, err := s.blockClient.UpdateVolume(ctx, &block.UpdateVolumeRequest{Id: id, Volume: vol})
	if err != nil {
		log.Errorf("Failed to update volume err: \n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Info("Update Volume successfully.")
	response.WriteEntity(res.Volume)
}

func (s *APIService) DeleteVolume(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "volume:delete") {
		return
	}

	id := request.PathParameter("id")
	log.Infof("Received request for deleting volume: %s\n", id)

	ctx := common.InitCtxWithAuthInfo(request)

	res, err := s.blockClient.DeleteVolume(ctx, &block.DeleteVolumeRequest{Id: id})
	if err != nil {
		log.Errorf("Failed to delete volume err: \n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Info("Delete Volume successfully.")
	response.WriteEntity(res)
}
