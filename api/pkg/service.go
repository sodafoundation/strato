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
	"encoding/json"
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
	name := request.PathParameter("name")
	log.Logf("Received request for policy[name=%s] details.", name)
	ctx := context.Background()
	res, err := s.dataflowClient.GetPolicy(ctx, &dataflow.GetPolicyRequest{Name: name})
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	//For debug -- begin
	jsons, errs := json.Marshal(res)
	if errs != nil {
		log.Logf(errs.Error())
	}else {
		log.Logf("res: %s.\n", jsons)
	}
	//For debug -- end

	log.Log("Get policy details successfully.")
	response.WriteEntity(res)
}

func (s *APIService) CreatePolicy(request *restful.Request, response *restful.Response) {
	//name := request.PathParameter("name")
	log.Logf("Received request for create policy.\n")
	ctx := context.Background()
	pol := dataflow.Policy{}
	err := request.ReadEntity(&pol)
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		log.Logf("Read request body failed, err:%v.\n", err)
		return
	}

	//For debug --begin
	jsons, errs := json.Marshal(pol)
	if errs != nil {
		log.Logf(errs.Error())
	}else {
		log.Logf("Req body: %s.\n", jsons)
	}
	//For debug --end

	//name,_ := request.BodyParameter("name")
	//tenant,_ := request.BodyParameter("tenant")
	//desc,_ := request.BodyParameter("description")

	res, err := s.dataflowClient.CreatePolicy(ctx, &dataflow.CreatePolicyRequest{Pol:&pol})
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Log("Get policy details successfully.")
	response.WriteEntity(res)
}