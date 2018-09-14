package backend

import (
	"net/http"
	"strconv"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"
	"github.com/micro/go-micro/client"
	//	"github.com/micro/go-micro/errors"

	"github.com/opensds/go-panda/backend/proto"
	"github.com/opensds/go-panda/dataflow/proto"
	"github.com/opensds/go-panda/s3/proto"
	"golang.org/x/net/context"
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
	if request.QueryParameter("limit") != "" {
		limit, err := strconv.Atoi(request.QueryParameter("limit"))
		if err != nil {
			log.Logf("limit is invalid: %v", err)
			response.WriteError(http.StatusInternalServerError, err)
			return
		}
		listBackendRequest.Limit = int32(limit)
	}

	if request.QueryParameter("offset") != "" {
		offset, err := strconv.Atoi(request.QueryParameter("offset"))
		if err != nil {
			log.Logf("offset is invalid: %v", err)
			response.WriteError(http.StatusInternalServerError, err)
			return
		}
		listBackendRequest.Offset = int32(offset)
	}

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
