package service

import (
	"net/http"

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
	id := request.PathParameter("id")
	log.Logf("Received request for backend details: %s", id)
	ctx := context.Background()
	res, err := s.backendClient.GetBackend(ctx, &backend.GetBackendRequest{Id: id})
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Log("Get backend details successfully.")
	response.WriteEntity(res)
}

func (s *APIService) GetObject(request *restful.Request, response *restful.Response) {
	id := request.PathParameter("id")
	log.Logf("Received request for object details: %s", id)
	ctx := context.Background()
	res, err := s.s3Client.GetObject(ctx, &s3.GetObjectRequest{Id: id})
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Log("Get object details successfully.")
	response.WriteEntity(res)
}

func (s *APIService) GetPolicy(request *restful.Request, response *restful.Response) {
	id := request.PathParameter("id")
	log.Logf("Received request for policy details: %s", id)
	ctx := context.Background()
	res, err := s.dataflowClient.GetPolicy(ctx, &dataflow.GetPolicyRequest{Id: id})
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Log("Get policy details successfully.")
	response.WriteEntity(res)
}
