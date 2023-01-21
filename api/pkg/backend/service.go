// Copyright 2019 The OpenSDS Authors.
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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/v2/client"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	"github.com/opensds/multi-cloud/api/pkg/common"
	c "github.com/opensds/multi-cloud/api/pkg/context"
	"github.com/opensds/multi-cloud/api/pkg/filters/signature/credentials/keystonecredentials"
	"github.com/opensds/multi-cloud/api/pkg/policy"
	"github.com/opensds/multi-cloud/api/pkg/utils/cryptography"
	backend "github.com/opensds/multi-cloud/backend/proto"
	dataflow "github.com/opensds/multi-cloud/dataflow/proto"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	s3 "github.com/opensds/multi-cloud/s3/proto"
)

const (
	MICRO_ENVIRONMENT = "MICRO_ENVIRONMENT"
	K8S               = "k8s"

	backendService_Docker  = "backend"
	s3Service_Docker       = "s3"
	dataflowService_Docker = "dataflow"
	backendService_K8S     = "soda.multicloud.v1.backend"
	s3Service_K8S          = "soda.multicloud.v1.s3"
	dataflowService_K8S    = "soda.multicloud.v1.dataflow"
	PROJECTS_URI           = "/v3/projects"
	PROJECTS_QUERY_CONDN   = "?domain_id=default"
)

// Map of object storage providers supported by s3 services. Keeping a map
// to optimize search
var objectStorage = map[string]int{
	"aws-s3":               1,
	"azure-blob":           1,
	"ibm-cos":              1,
	"hw-obs":               1,
	"ceph-s3":              1,
	"gcp-s3":               1,
	"fusionstorage-object": 1,
	"yig":                  1,
	"alibaba-oss":          1,
	"sony-oda":             1,
}

type APIService struct {
	backendClient  backend.BackendService
	s3Client       s3.S3Service
	dataflowClient dataflow.DataFlowService
}

type EnCrypter struct {
	Algo      string `json:"algo,omitempty"`
	Access    string `json:"access,omitempty"`
	PlainText string `json:"plaintext,omitempty"`
}

type DeCrypter struct {
	CipherText string `json:"ciphertext,omitempty"`
}

func isObjectStorage(storage string) bool {
	_, ok := objectStorage[storage]
	return ok
}

func NewAPIService(c client.Client) *APIService {

	backendService := backendService_Docker
	s3Service := s3Service_Docker
	dataflowService := dataflowService_Docker

	if os.Getenv(MICRO_ENVIRONMENT) == K8S {
		backendService = backendService_K8S
		s3Service = s3Service_K8S
		dataflowService = dataflowService_K8S
	}
	return &APIService{
		backendClient:  backend.NewBackendService(backendService, c),
		s3Client:       s3.NewS3Service(s3Service, c),
		dataflowClient: dataflow.NewDataFlowService(dataflowService, c),
	}
}

func (s *APIService) GetBackend(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "backend:get") {
		return
	}
	log.Infof("Received request for backend details: %s\n", request.PathParameter("id"))
	id := request.PathParameter("id")

	ctx := common.InitCtxWithAuthInfo(request)
	res, err := s.backendClient.GetBackend(ctx, &backend.GetBackendRequest{Id: id})

	if err != nil {
		log.Errorf("failed to get backend details: %v %s\n", err, err.Error())
		if strings.Contains(err.Error(), "invalid input to ObjectIdHex") {
			errMsg := fmt.Sprintf("Invalid backend id: %s", id)
			response.WriteError(http.StatusNotFound, errors.New(errMsg))
		} else {
			response.WriteError(http.StatusInternalServerError, err)
		}
		return
	}

	// do not return sensitive information
	res.Backend.Access = ""
	res.Backend.Security = ""

	log.Info("Get backend details successfully.")
	response.WriteEntity(res.Backend)
}

func (s *APIService) listBackendDefault(ctx context.Context, request *restful.Request, response *restful.Response) {
	listBackendRequest := &backend.ListBackendRequest{}

	limit, offset, err := common.GetPaginationParam(request)
	if err != nil {
		log.Errorf("get pagination parameters failed: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	listBackendRequest.Limit = limit
	listBackendRequest.Offset = offset

	sortKeys, sortDirs, err := common.GetSortParam(request)
	if err != nil {
		log.Errorf("get sort parameters failed: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	listBackendRequest.SortKeys = sortKeys
	listBackendRequest.SortDirs = sortDirs

	filterOpts := []string{"name", "type", "region"}
	filter, err := common.GetFilter(request, filterOpts)
	if err != nil {
		log.Errorf("get filter failed: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	listBackendRequest.Filter = filter

	res, err := s.backendClient.ListBackend(ctx, listBackendRequest)
	if err != nil {
		log.Errorf("failed to list backends: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	// do not return sensitive information
	for _, v := range res.Backends {
		v.Access = ""
		v.Security = ""
	}

	log.Info("List backends successfully.")
	response.WriteEntity(res)
}

func (s *APIService) FilterBackendByTier(ctx context.Context, request *restful.Request, response *restful.Response,
	tier int32) {
	// Get those backend type which supporte the specific tier.
	req := s3.GetBackendTypeByTierRequest{Tier: tier}
	res, _ := s.s3Client.GetBackendTypeByTier(context.Background(), &req)
	req1 := &backend.ListBackendRequest{}
	resp := &backend.ListBackendResponse{}
	for _, v := range res.Types {
		// Get backends with specific backend type.
		filter := make(map[string]string)
		filter["type"] = v
		req1.Filter = filter
		res1, err := s.backendClient.ListBackend(ctx, req1)
		if err != nil {
			log.Errorf("failed to list backends of type[%s]: %v\n", v, err)
			response.WriteError(http.StatusInternalServerError, err)
		}
		if len(res1.Backends) != 0 {
			resp.Backends = append(resp.Backends, res1.Backends...)
		}
	}
	//TODO: Need to consider pagination

	// do not return sensitive information
	for _, v := range resp.Backends {
		v.Access = ""
		v.Security = ""
	}

	log.Info("fiterBackendByTier backends successfully.")
	response.WriteEntity(resp)
}

func (s *APIService) ListBackend(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "backend:list") {
		return
	}
	log.Info("Received request for backend list.")

	ctx := common.InitCtxWithAuthInfo(request)
	para := request.QueryParameter("tier")
	if para != "" { //List those backends which support the specific tier.
		tier, err := strconv.Atoi(para)
		if err != nil {
			log.Errorf("list backends with tier as filter, but tier[%v] is invalid\n", tier)
			response.WriteError(http.StatusBadRequest, errors.New("invalid service plan"))
			return
		}
		s.FilterBackendByTier(ctx, request, response, int32(tier))
	} else {
		s.listBackendDefault(ctx, request, response)
	}
}

func (s *APIService) CreateBackend(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "backend:create") {
		return
	}
	log.Info("Received request for creating backend.")
	backendDetail := &backend.BackendDetail{}
	err := request.ReadEntity(&backendDetail)
	if err != nil {
		log.Errorf("failed to read request body: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	ctx := common.InitCtxWithAuthInfo(request)
	actx := request.Attribute(c.KContext).(*c.Context)
	backendDetail.TenantId = actx.TenantId
	backendDetail.UserId = actx.UserId

	storageTypes, err := s.listStorageType(ctx, request, response)
	if err != nil {
		log.Errorf("failed to list backend storage type: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	foundType := typeExists(storageTypes.Types, backendDetail.Type)
	if !foundType {
		log.Errorf("failed to retrieve backend type: %v\n", err)
		response.WriteError(http.StatusBadRequest, err)
		return
	}

	backendDetailS3 := &s3.BackendDetailS3{}
	backendDetailS3.Id = backendDetail.Id
	backendDetailS3.Name = backendDetail.Name
	backendDetailS3.Type = backendDetail.Type
	backendDetailS3.Region = backendDetail.Region
	backendDetailS3.Endpoint = backendDetail.Endpoint
	backendDetailS3.BucketName = backendDetail.BucketName
	backendDetailS3.Access = backendDetail.Access
	backendDetailS3.Security = backendDetail.Security

	// This backend check will be called only for object storage
	if isObjectStorage(backendDetail.Type) {
		_, err = s.s3Client.BackendCheck(ctx, backendDetailS3)
		if err != nil {
			log.Errorf("failed to create backend due to wrong credentials: %v", err)
			err1 := errors.New("Failed to register backend due to invalid credentials.")
			response.WriteError(http.StatusBadRequest, err1)
			return
		}
	}

	res, err := s.backendClient.CreateBackend(ctx, &backend.CreateBackendRequest{Backend: backendDetail})
	if err != nil {
		log.Errorf("failed to create backend: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Info("Created backend successfully.")
	response.WriteEntity(res.Backend)
}

func typeExists(slice []*backend.TypeDetail, inputType string) bool {
	for _, item := range slice {
		if item.Name == inputType {
			log.Debug("backend type is valid")
			return true
		}
	}
	return false
}

func (s *APIService) UpdateBackend(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "backend:update") {
		return
	}
	log.Infof("Received request for updating backend: %v\n", request.PathParameter("id"))
	updateBackendRequest := backend.UpdateBackendRequest{Id: request.PathParameter("id")}
	err := request.ReadEntity(&updateBackendRequest)
	if err != nil {
		log.Errorf("failed to read request body: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	ctx := common.InitCtxWithAuthInfo(request)
	res, err := s.backendClient.UpdateBackend(ctx, &updateBackendRequest)
	if err != nil {
		log.Errorf("failed to get backend details: %v %s\n", err, err.Error())
		if strings.Contains(err.Error(), "invalid input to ObjectIdHex") {
			errMsg := fmt.Sprintf("Invalid backend id: %s", request.PathParameter("id"))
			response.WriteError(http.StatusNotFound, errors.New(errMsg))
		} else {
			response.WriteError(http.StatusInternalServerError, err)
		}
		return
	}

	log.Info("Update backend successfully.")
	response.WriteEntity(res.Backend)
}

func (s *APIService) DeleteBackend(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "backend:delete") {
		return
	}
	id := request.PathParameter("id")
	log.Infof("Received request for deleting backend: %s\n", id)

	ctx := common.InitCtxWithAuthInfo(request)
	result, err := s.backendClient.GetBackend(ctx, &backend.GetBackendRequest{Id: id})
	if err != nil {
		log.Errorf("failed to get backend details: %v %s\n", err, err.Error())
		if strings.Contains(err.Error(), "invalid input to ObjectIdHex") {
			errMsg := fmt.Sprintf("Invalid backend id: %s", request.PathParameter("id"))
			response.WriteError(http.StatusNotFound, errors.New(errMsg))
		} else {
			response.WriteError(http.StatusInternalServerError, err)
		}
		return
	}
	backendname := result.Backend.Name
	res, err := s.s3Client.ListBuckets(ctx, &s3.ListBucketsRequest{Filter: map[string]string{"location": backendname}})
	if err != nil {
		log.Errorf("failed to bucket list details: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	count := len(res.Buckets)

	if count == 0 {
		_, err := s.backendClient.DeleteBackend(ctx, &backend.DeleteBackendRequest{Id: id})
		if err != nil {
			log.Errorf("failed to delete backend: %v\n", err)
			response.WriteError(http.StatusInternalServerError, err)
			return
		}
		log.Info("Delete backend successfully.")
		response.WriteHeader(http.StatusOK)
		return
	} else {
		log.Info("the backend can not be deleted, need to delete bucket first.")
		response.WriteError(http.StatusInternalServerError, BackendDeleteError.Error())
		return
	}
}

func (s *APIService) ListType(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "type:list") {
		return
	}
	log.Info("Received request for backends type list.")
	ctx := context.Background()
	storageTypes, err := s.listStorageType(ctx, request, response)
	if err != nil {
		log.Errorf("failed to list types of backend: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	log.Info("List types successfully.")
	response.WriteEntity(storageTypes)
}

func (s *APIService) EncryptData(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "backend:encrypt") {
		return
	}
	log.Info("Received request for encrypting data.")
	encrypter := &EnCrypter{}
	err := request.ReadEntity(&encrypter)
	if err != nil {
		log.Errorf("failed to read request body: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	credential, err := keystonecredentials.NewCredentialsClient(encrypter.Access).Get()
	if err != nil {
		log.Error(err)
		return
	}

	aes := cryptography.NewSymmetricKeyEncrypter(encrypter.Algo)
	cipherText, err := aes.Encrypter(encrypter.PlainText, []byte(credential.SecretAccessKey))
	if err != nil {
		log.Error(err)
		return
	}

	log.Info("Encrypt data successfully.")
	response.WriteEntity(DeCrypter{CipherText: cipherText})
}

func (s *APIService) listStorageType(ctx context.Context, request *restful.Request, response *restful.Response) (*backend.ListTypeResponse, error) {
	listTypeRequest := &backend.ListTypeRequest{}

	limit, offset, err := common.GetPaginationParam(request)
	if err != nil {
		log.Errorf("get pagination parameters failed: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return nil, err
	}
	listTypeRequest.Limit = limit
	listTypeRequest.Offset = offset

	sortKeys, sortDirs, err := common.GetSortParam(request)
	if err != nil {
		log.Errorf("get sort parameters failed: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return nil, err
	}
	listTypeRequest.SortKeys = sortKeys
	listTypeRequest.SortDirs = sortDirs

	filterOpts := []string{"name"}
	filter, err := common.GetFilter(request, filterOpts)
	if err != nil {
		log.Errorf("get filter failed: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return nil, err
	}
	listTypeRequest.Filter = filter

	storageTypes, err := s.backendClient.ListType(ctx, listTypeRequest)
	return storageTypes, err
}

func (s *APIService) CheckInvalidBackends(ctx context.Context, backends []string) ([]string, error) {
	var invalidBackends []string
	listBackendRequest := &backend.ListBackendRequest{}
	result, err := s.backendClient.ListBackend(ctx, listBackendRequest)
	if err != nil {
		return invalidBackends, err
	}
	for _, backendId := range backends {
		exists := false
		for _, bknd := range result.Backends {
			if backendId == bknd.Id {
				exists = true
				break
			}
		}
		if exists == false {
			invalidBackends = append(invalidBackends, backendId)
		}
	}
	return invalidBackends, nil
}

//response of projects
type ProjectsResponse struct {
	Links struct {
		Self     string      `json:"self"`
		Previous interface{} `json:"previous"`
		Next     interface{} `json:"next"`
	} `json:"links"`
	Projects []struct {
		IsDomain    bool   `json:"is_domain"`
		Description string `json:"description"`
		Links       struct {
			Self string `json:"self"`
		} `json:"links"`
		Tags     []interface{} `json:"tags"`
		Enabled  bool          `json:"enabled"`
		ID       string        `json:"id"`
		ParentID string        `json:"parent_id"`
		DomainID string        `json:"domain_id"`
		Name     string        `json:"name"`
	} `json:"projects"`
}

///function to validate tenant:
func ValidateTenant(authToken, tenantId string) bool {
	osAuthUrl := os.Getenv("OS_AUTH_URL")
	if osAuthUrl == "" {
		errMsg := fmt.Sprintf("error in getting OS_AUTH_URL")
		log.Error(errMsg)
		return false
	}
	getreq, err := http.NewRequest("GET", osAuthUrl+PROJECTS_URI+PROJECTS_QUERY_CONDN, bytes.NewBuffer(nil))
	errMsg := fmt.Sprintf("error in getting all projects:")
	if err != nil {
		log.Error(errMsg)
		return false
	}
	getreq.Header.Add("X-Auth-Token", authToken)
	getreq.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	var validationResponse *http.Response
	validationResponse, err = client.Do(getreq)
	if err != nil {
		log.Error(errMsg)
		return false
	}
	defer validationResponse.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(validationResponse.Body)
	var data *ProjectsResponse
	json.Unmarshal(bodyBytes, &data)
	for _, project := range data.Projects {
		if project.ID == tenantId {
			return true
		}
	}
	return false
}
