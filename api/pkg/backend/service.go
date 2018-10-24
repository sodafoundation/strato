// Copyright (c) 2018 Huawei Technologies Co., Ltd. All Rights Reserved.
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

package backend

import (
	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"
	"github.com/micro/go-micro/client"
	"github.com/opensds/multi-cloud/api/pkg/common"
	"github.com/opensds/multi-cloud/backend/proto"
	"github.com/opensds/multi-cloud/dataflow/proto"
	"github.com/opensds/multi-cloud/s3/proto"
	"golang.org/x/net/context"
	"net/http"
)

const (
	backendService  = "backend"
	s3Service       = "s3"
	dataflowService = "dataflow"
)

type APIService struct {
	backendClient  backend.BackendService
	s3Client       s3.S3Service
	dataflowClient dataflow.DataFlowService
}

func NewAPIService(c client.Client) *APIService {
	return &APIService{
		backendClient:  backend.NewBackendService(backendService, c),
		s3Client:       s3.NewS3Service(s3Service, c),
		dataflowClient: dataflow.NewDataFlowService(dataflowService, c),
	}
}

func (s *APIService) GetBackend(request *restful.Request, response *restful.Response) {
	log.Logf("Received request for backend details: %s", request.PathParameter("id"))
	id := request.PathParameter("id")
	ctx := context.Background()
	res, err := s.backendClient.GetBackend(ctx, &backend.GetBackendRequest{Id: id})
	if err != nil {
		log.Logf("Failed to get backend details: %v", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Log("Get backend details successfully.")
	response.WriteEntity(res.Backend)
}

func (s *APIService) ListBackend(request *restful.Request, response *restful.Response) {
	log.Log("Received request for backend list.")
	listBackendRequest := &backend.ListBackendRequest{}

	limit, offset, err := common.GetPaginationParam(request)
	if err != nil {
		log.Logf("Get pagination parameters failed: %v", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	listBackendRequest.Limit = limit
	listBackendRequest.Offset = offset

	sortKeys, sortDirs, err := common.GetSortParam(request)
	if err != nil {
		log.Logf("Get sort parameters failed: %v", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	listBackendRequest.SortKeys = sortKeys
	listBackendRequest.SortDirs = sortDirs

	filterOpts := []string{"name", "type", "region"}
	filter, err := common.GetFilter(request, filterOpts)
	if err != nil {
		log.Logf("Get filter failed: %v", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	listBackendRequest.Filter = filter

	ctx := context.Background()
	res, err := s.backendClient.ListBackend(ctx, listBackendRequest)
	if err != nil {
		log.Logf("Failed to list backends: %v", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Log("List backends successfully.")
	response.WriteEntity(res)
}

func (s *APIService) CreateBackend(request *restful.Request, response *restful.Response) {
	log.Log("Received request for creating backend.")
	backendDetail := &backend.BackendDetail{}
	err := request.ReadEntity(&backendDetail)
	if err != nil {
		log.Logf("Failed to read request body: %v", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	ctx := context.Background()
	res, err := s.backendClient.CreateBackend(ctx, &backend.CreateBackendRequest{Backend: backendDetail})
	if err != nil {
		log.Logf("Failed to create backend: %v", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Log("Create backend successfully.")
	response.WriteEntity(res.Backend)
}

func (s *APIService) UpdateBackend(request *restful.Request, response *restful.Response) {
	log.Logf("Received request for updating backend: %v", request.PathParameter("id"))
	updateBackendRequest := backend.UpdateBackendRequest{Id: request.PathParameter("id")}
	err := request.ReadEntity(&updateBackendRequest)
	if err != nil {
		log.Logf("Failed to read request body: %v", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	ctx := context.Background()
	res, err := s.backendClient.UpdateBackend(ctx, &updateBackendRequest)
	if err != nil {
		log.Logf("Failed to update backend: %v", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Log("Update backend successfully.")
	response.WriteEntity(res.Backend)
}

func (s *APIService) DeleteBackend(request *restful.Request, response *restful.Response) {
	log.Logf("Received request for deleting backend: %s", request.PathParameter("id"))
	ctx := context.Background()
	_, err := s.backendClient.DeleteBackend(ctx, &backend.DeleteBackendRequest{Id: request.PathParameter("id")})
	if err != nil {
		log.Logf("Failed to delete backend: %v", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Log("Delete backend successfully.")
	response.WriteHeader(http.StatusOK)
}

func (s *APIService) ListType(request *restful.Request, response *restful.Response) {
	log.Log("Received request for backend type list.")
	listTypeRequest := &backend.ListTypeRequest{}

	limit, offset, err := common.GetPaginationParam(request)
	if err != nil {
		log.Logf("Get pagination parameters failed: %v", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	listTypeRequest.Limit = limit
	listTypeRequest.Offset = offset

	sortKeys, sortDirs, err := common.GetSortParam(request)
	if err != nil {
		log.Logf("Get sort parameters failed: %v", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	listTypeRequest.SortKeys = sortKeys
	listTypeRequest.SortDirs = sortDirs

	filterOpts := []string{"name"}
	filter, err := common.GetFilter(request, filterOpts)
	if err != nil {
		log.Logf("Get filter failed: %v", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	listTypeRequest.Filter = filter

	ctx := context.Background()
	res, err := s.backendClient.ListType(ctx, listTypeRequest)
	if err != nil {
		log.Logf("Failed to list types: %v", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Log("List types successfully.")
	response.WriteEntity(res)
}
