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

package file

import (
	"context"
	"net/http"
	"time"

	"github.com/soda/multi-cloud/api/pkg/common"
	c "github.com/soda/multi-cloud/api/pkg/context"
	"github.com/soda/multi-cloud/api/pkg/policy"
	backend "github.com/soda/multi-cloud/backend/proto"
	"github.com/soda/multi-cloud/contrib/utils"
	"github.com/soda/multi-cloud/file/pkg/model"
	file "github.com/soda/multi-cloud/file/proto"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/v2/client"
	log "github.com/sirupsen/logrus"
)

const (
	fileService    = "file"
	backendService = "backend"
)

type APIService struct {
	fileClient    file.FileService
	backendClient backend.BackendService
}

func NewAPIService(c client.Client) *APIService {
	return &APIService{
		fileClient:    file.NewFileService(fileService, c),
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

func (s *APIService) prepareFileShareRequest(request *restful.Request,
	response *restful.Response) *file.ListFileShareRequest {

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

	return &file.ListFileShareRequest{
		Limit:    limit,
		Offset:   offset,
		SortKeys: sortKeys,
		SortDirs: sortDirs,
		Filter:   filter,
	}
}

func (s *APIService) ListFileShare(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "fileshare:list") {
		return
	}
	log.Info("Received request for File Share List.")

	ctx := common.InitCtxWithAuthInfo(request)
	ctx, _ = context.WithTimeout(ctx, 5*time.Minute)

	backendId := request.QueryParameter(common.REQUEST_PATH_BACKEND_ID)

	var listFileShareRequest *file.ListFileShareRequest

	if backendId != "" {
		//List fileshares for the backend.
		listFileShareRequest = s.listFileShareByBackend(ctx, request, response, backendId)
	} else {
		//List all fileshares.
		listFileShareRequest = s.listFileShareDefault(ctx, request, response)
	}

	if listFileShareRequest == nil {
		return
	}

	res, err := s.fileClient.ListFileShare(ctx, listFileShareRequest)
	if err != nil {
		WriteError(response, "List FileShares failed: %v\n", http.StatusInternalServerError, err)
		return
	}

	log.Info("List FileShares successfully.")

	err = response.WriteEntity(res)
	if err != nil {
		log.Errorf("Response write entity failed: %v\n", err)
		return
	}
}

func (s *APIService) listFileShareDefault(ctx context.Context, request *restful.Request,
	response *restful.Response) *file.ListFileShareRequest {

	return s.prepareFileShareRequest(request, response)
}

func (s *APIService) listFileShareByBackend(ctx context.Context, request *restful.Request, response *restful.Response,
	backendId string) *file.ListFileShareRequest {

	if s.checkBackendExists(ctx, request, response, backendId) != nil {
		return nil
	}

	filterQueryOpts := []string{"backendId"}
	filterQuery, err := common.GetFilter(request, filterQueryOpts)
	if err != nil {
		WriteError(response, "Get filter failed: %v\n", http.StatusBadRequest, err)
		return nil
	}

	listFileShareRequest := s.prepareFileShareRequest(request, response)

	if listFileShareRequest == nil {
		return nil
	}

	if UpdateFilter(listFileShareRequest.Filter, filterQuery) != nil {
		log.Errorf("Update filter failed: %v\n", err)
		return nil
	}

	return listFileShareRequest
}

func (s *APIService) GetFileShare(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "fileshare:get") {
		return
	}
	log.Infof("Received request for file share details: %s\n", request.PathParameter("id"))

	id := request.PathParameter("id")

	ctx := common.InitCtxWithAuthInfo(request)
	ctx, _ = context.WithTimeout(ctx, 5*time.Minute)

	res, err := s.fileClient.GetFileShare(ctx, &file.GetFileShareRequest{Id: id})
	if err != nil {
		log.Errorf("Failed to get file share details err: %s\n", err)
		response.WriteError(http.StatusNotFound, err)
		return
	}

	log.Info("Get FileShare details successfully.")
	response.WriteEntity(res.Fileshare)
}

func (s *APIService) CreateFileShare(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "fileshare:create") {
		return
	}
	log.Info("Received request for creating file share.")

	fileshare := &model.FileShare{}

	err := request.ReadEntity(&fileshare)
	if err != nil {
		log.Errorf("failed to read request body err: %s \n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	var tags []*file.Tag
	for _, tag := range fileshare.Tags {
		tags = append(tags, &file.Tag{
			Key:   tag.Key,
			Value: tag.Value,
		})
	}

	metadata, err := utils.ConvertMapToStruct(fileshare.Metadata)
	if err != nil {
		log.Errorf("failed to convert metadata map to struct: %v error : %s\n", fileshare.Metadata, err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	fs := &file.FileShare{
		Name:             fileshare.Name,
		Region:           fileshare.Region,
		BackendId:        fileshare.BackendId,
		AvailabilityZone: fileshare.AvailabilityZone,
		Tags:             tags,
		Protocols:        fileshare.Protocols,
		Metadata:         metadata,
	}

	//TODO: The following checks can be updated once we have swagger validation in place
	if fileshare.Id != "" {
		fs.Id = fileshare.Id.Hex()
	}

	if fileshare.Size != nil {
		fs.Size = *fileshare.Size
	}

	if fileshare.Description != "" {
		fs.Description = fileshare.Description
	}

	if fileshare.Type != "" {
		fs.Type = fileshare.Type
	}

	if fileshare.Encrypted != nil {
		fs.Encrypted = *fileshare.Encrypted

		if fs.Encrypted {
			fs.EncryptionSettings = fileshare.EncryptionSettings
		}
	}

	ctx := common.InitCtxWithAuthInfo(request)
	ctx, _ = context.WithTimeout(ctx, 5*time.Minute)

	if s.checkBackendExists(ctx, request, response, fs.BackendId) != nil {
		return
	}

	actx := request.Attribute(c.KContext).(*c.Context)
	fs.TenantId = actx.TenantId
	fs.UserId = actx.UserId

	res, err := s.fileClient.CreateFileShare(ctx, &file.CreateFileShareRequest{Fileshare: fs})
	if err != nil {
		log.Errorf("Failed to create file share err: %s\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Info("Create file share successfully.")
	response.WriteEntity(res.Fileshare)
}

func (s *APIService) UpdateFileShare(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "fileshare:put") {
		return
	}
	log.Info("Received request for updating file share.")

	fileshare := &model.FileShare{}

	err := request.ReadEntity(&fileshare)
	if err != nil {
		log.Errorf("Failed to read request body err: %s \n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	fs := &file.FileShare{}

	if fileshare.Description != "" {
		fs.Description = fileshare.Description
	}

	if fileshare.Metadata != nil || len(fileshare.Metadata) != 0 {
		metadata, err := utils.ConvertMapToStruct(fileshare.Metadata)
		if err != nil {
			log.Errorf("Failed to parse metadata: %v , error : %s\n", fileshare.Metadata, err)
			response.WriteError(http.StatusInternalServerError, err)
			return
		}

		fs.Metadata = metadata
	}

	if fileshare.Tags != nil {
		var tags []*file.Tag
		for _, tag := range fileshare.Tags {
			tags = append(tags, &file.Tag{
				Key:   tag.Key,
				Value: tag.Value,
			})
		}
		fs.Tags = tags
	}

	if fileshare.Size != nil {
		fs.Size = *fileshare.Size
	}

	id := request.PathParameter("id")

	ctx := common.InitCtxWithAuthInfo(request)
	ctx, _ = context.WithTimeout(ctx, 5*time.Minute)

	actx := request.Attribute(c.KContext).(*c.Context)
	fs.TenantId = actx.TenantId
	fs.UserId = actx.UserId

	res, err := s.fileClient.UpdateFileShare(ctx, &file.UpdateFileShareRequest{Id: id, Fileshare: fs})
	if err != nil {
		log.Errorf("Failed to create file share err: %s\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Info("Create FileShare successfully.")
	response.WriteEntity(res.Fileshare)
}

func (s *APIService) DeleteFileShare(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "fileshare:delete") {
		return
	}

	id := request.PathParameter("id")
	log.Infof("Received request for deleting file share: %s\n", id)

	ctx := common.InitCtxWithAuthInfo(request)
	ctx, _ = context.WithTimeout(ctx, 5*time.Minute)

	res, err := s.fileClient.DeleteFileShare(ctx, &file.DeleteFileShareRequest{Id: id})
	if err != nil {
		log.Errorf("Failed to delete file share err: %s\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Info("Delete file share successfully.")
	response.WriteEntity(res)
}
