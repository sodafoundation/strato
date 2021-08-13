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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/v2/client"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	"github.com/opensds/multi-cloud/api/pkg/common"
	c "github.com/opensds/multi-cloud/api/pkg/context"
	"github.com/opensds/multi-cloud/api/pkg/filters/signature/credentials/keystonecredentials"
	"github.com/opensds/multi-cloud/api/pkg/policy"
	utils "github.com/opensds/multi-cloud/api/pkg/utils"
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
		log.Errorf("failed to get backend details: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
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
		log.Errorf("failed to update backend: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
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
		log.Errorf("failed to get backend details: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
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

//tiering functions
func (s *APIService) CreateTier(request *restful.Request, response *restful.Response) {
	log.Info("Received request for creating service plan.")
	if !policy.Authorize(request, response, "tier:create") {
		return
	}
	tier := &backend.Tier{}
	body := utils.ReadBody(request)
	err := json.Unmarshal(body, &tier)
	if err != nil {
		log.Error("error occurred while decoding body", err)
		response.WriteError(http.StatusBadRequest, nil)
		return
	}
	if len(tier.Name) == 0 {
		errMsg := fmt.Sprintf("\"Name\" is required parameter should not be empty")
		log.Error(errMsg)
		response.WriteError(http.StatusBadRequest, errors.New(errMsg))
		return
	}

	ctx := common.InitCtxWithAuthInfo(request)
	tier.TenantId = request.PathParameter("tenantId")

	// ValidationCheck:1:: Name should be unique. Repeated name not allowed
	tierResult, err := s.backendClient.ListTiers(ctx, &backend.ListTierRequest{
		Filter: map[string]string{"name": tier.Name},
	})
	if err != nil {
		log.Errorf("failed to list Service plans: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	if len(tierResult.Tiers) > 0 {
		errMsg := fmt.Sprintf("name: %v is already exists.", tier.Name)
		log.Error(errMsg)
		response.WriteError(http.StatusBadRequest, errors.New(errMsg))
		return
	}

	// ValidationCheck:2:: Check tenants in request should not be repeated
	dupTenants := utils.IsDuplicateItemExist(tier.Tenants)
	if len(dupTenants) != 0 {
		errMsg := fmt.Sprintf("duplicate tenants are found in request: %v", dupTenants)
		log.Error(errMsg)
		response.WriteError(http.StatusBadRequest, errors.New(errMsg))
		return
	}

	// ValidationCheck:3:: Check backends in request should not be repeated
	dupBackends := utils.IsDuplicateItemExist(tier.Backends)
	if len(dupBackends) != 0 {
		errMsg := fmt.Sprintf("duplicate backends are found in request: %v", dupBackends)
		log.Error(errMsg)
		response.WriteError(http.StatusBadRequest, errors.New(errMsg))
		return
	}

	// ValidationCheck:4:: validation of backends whether they are valid or not
	invalidBackends, err := s.CheckInvalidBackends(ctx, tier.Backends)
	if err != nil {
		log.Errorf("failed to find invalidBackends: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	if len(invalidBackends) > 0 {
		errMsg := fmt.Sprintf("invalid backends are found in request: %v", invalidBackends)
		log.Error(errMsg)
		response.WriteError(http.StatusBadRequest, errors.New(errMsg))
		return

	}

	// Now, tier can be created with backends for tenants
	res, err := s.backendClient.CreateTier(ctx, &backend.CreateTierRequest{Tier: tier})
	if err != nil {
		log.Errorf("failed to create service plan: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Info("Created service plan successfully.")
	response.WriteEntity(res.Tier)

}

//here backendId can be updated
func (s *APIService) UpdateTier(request *restful.Request, response *restful.Response) {
	log.Infof("Received request for updating service plan: %v\n", request.PathParameter("id"))
	if !policy.Authorize(request, response, "tier:update") {
		return
	}
	id := request.PathParameter("id")
	updateTier := backend.UpdateTier{Id: id}
	body := utils.ReadBody(request)
	err := json.Unmarshal(body, &updateTier)
	if err != nil {
		log.Error("error occurred while decoding body", err)
		response.WriteError(http.StatusBadRequest, nil)
		return
	}

	ctx := common.InitCtxWithAuthInfo(request)

	// ValidationCheck::BackendExists:: Check whether tier id exists or not
	log.Info("ValidationCheck::BackendExists:: Check whether service plan id exists or not")
	res, err := s.backendClient.GetTier(ctx, &backend.GetTierRequest{Id: id})
	if err != nil {
		errMsg := fmt.Sprintf("failed to get service plan details for id: %v\n, err: %v\n", id, err)
		log.Error(errMsg)
		response.WriteError(http.StatusBadRequest, errors.New(errMsg))
		return
	}
	log.Info("Backend: %v exists", id)

	// ValidationCheck:DuplicateBackendsInAddBackends:: Check AddBackends in request should not be repeated
	log.Info("ValidationCheck:Duplicate Backends In AddBackends")
	dupBackends := utils.IsDuplicateItemExist(updateTier.AddBackends)
	if len(dupBackends) > 0 {
		errMsg := fmt.Sprintf("duplicate backends are found in AddBackends request: %v", dupBackends)
		log.Error(errMsg)
		response.WriteError(http.StatusBadRequest, errors.New(errMsg))
		return
	}

	// ValidationCheck:DuplicateBackendsInAddBackends:: Check DeleteBackends in request should not be repeated
	log.Info("ValidationCheck: Duplicate Backends In DeleteBackends")
	dupBackends = utils.IsDuplicateItemExist(updateTier.DeleteBackends)
	if len(dupBackends) > 0 {
		errMsg := fmt.Sprintf("duplicate backends are found in DeleteBackends request: %v", dupBackends)
		log.Error(errMsg)
		response.WriteError(http.StatusBadRequest, errors.New(errMsg))
		return
	}

	// ValidationCheck:2:: In the same request, same backend can not be
	// present in AddBackends and DeleteBackends both
	log.Info("ValidationCheck::dupItemFoundInDelandAddBackends:: Check whether tier id exists or not")
	dupItemFoundInDelandAddBackends := utils.CompareDeleteAndAddList(updateTier.AddBackends,
		updateTier.DeleteBackends)
	if len(dupItemFoundInDelandAddBackends) > 0 {
		errMsg := fmt.Sprintf("some backends found in AddBackends and DeleteBackends"+
			"in same request, which is not allowed: %v", dupItemFoundInDelandAddBackends)
		log.Error(errMsg)
		response.WriteError(http.StatusBadRequest, errors.New(errMsg))
		return
	}
	log.Info("No duplicate backends found in Delete and Add Backends")

	// ValidationCheck:DuplicateTenants:: Check tenants in AddTenants request should not be repeated
	log.Info("ValidationCheck::DuplicateTenants should not allowed in AddTenants")
	dupTenants := utils.IsDuplicateItemExist(updateTier.AddTenants)
	if len(dupTenants) > 0 {
		errMsg := fmt.Sprintf("duplicate tenants are found in AddTenants request: %v", dupTenants)
		log.Error(errMsg)
		response.WriteError(http.StatusBadRequest, errors.New(errMsg))
		return
	}

	// ValidationCheck:2:: Check tenants in DeleteTenants request should not be repeated
	log.Info("ValidationCheck::DuplicateTenants should not allowed in DeleteTenants")
	dupTenants = utils.IsDuplicateItemExist(updateTier.DeleteTenants)
	if len(dupTenants) > 0 {
		errMsg := fmt.Sprintf("duplicate tenants are found in DeleteTenants request: %v", dupTenants)
		log.Error(errMsg)
		response.WriteError(http.StatusBadRequest, errors.New(errMsg))
		return
	}

	// ValidationCheck:SameBackendsNotAllowedInBoth:: In the same request, same backend can not be
	// present in AddBackends and DeleteBackends both
	log.Info("ValidationCheck:Same Tenants should Not Allowed In Both")
	dupItemFoundInDelandAddTenants := utils.CompareDeleteAndAddList(updateTier.AddTenants,
		updateTier.DeleteTenants)
	if len(dupItemFoundInDelandAddTenants) > 0 {
		errMsg := fmt.Sprintf("some backends found in AddBackends and DeleteBackends"+
			"in same request, which is not allowed: %v", dupItemFoundInDelandAddTenants)
		log.Error(errMsg)
		response.WriteError(http.StatusBadRequest, errors.New(errMsg))
		return
	}

	// ValidationCheck:5:: validation of backends whether they are valid or not
	log.Info("Check for invalid backends")
	invalidBackends, err := s.CheckInvalidBackends(ctx, updateTier.AddBackends)
	if err != nil {
		log.Errorf("failed to find invalidBackends: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	if len(invalidBackends) > 0 {
		errMsg := fmt.Sprintf("invalid backends are found in AddBackends request: %v", invalidBackends)
		log.Error(errMsg)
		response.WriteError(http.StatusBadRequest, errors.New(errMsg))
		return

	}

	log.Info("Check whether backends already exists before adding")
	backendsExist := utils.ResourcesAlreadyExists(res.Tier.Backends, updateTier.AddBackends)
	if len(backendsExist) > 0 {
		errMsg := fmt.Sprintf("some backends in AddBackends request: %v already exists in service plan", backendsExist)
		log.Error(errMsg)
		response.WriteError(http.StatusBadRequest, errors.New(errMsg))
		return
	}

	log.Info("Check backends should exists before removing")
	backendsNotExist := utils.ResourcesCheckBeforeRemove(res.Tier.Backends, updateTier.DeleteBackends)
	if len(backendsNotExist) > 0 {
		errMsg := fmt.Sprintf("failed to update service plan because delete backends: %v doesnt exist in service plan", backendsNotExist)
		log.Error(errMsg)
		response.WriteError(http.StatusBadRequest, errors.New(errMsg))
		return
	}

	log.Info("Check whether Tenant already exists before adding")
	tenantExist := utils.ResourcesAlreadyExists(res.Tier.Tenants, updateTier.AddTenants)
	if len(tenantExist) > 0 {
		errMsg := fmt.Sprintf("some backends in AddBackends request: %v already exists in service plan", tenantExist)
		log.Error(errMsg)
		response.WriteError(http.StatusBadRequest, errors.New(errMsg))
		return
	}
	log.Info("Check Tenant should exists before removing")
	tenantNotExist := utils.ResourcesCheckBeforeRemove(res.Tier.Tenants, updateTier.DeleteTenants)
	if len(tenantNotExist) > 0 {
		errMsg := fmt.Sprintf("failed to update service plan because delete backends: %v doesnt exist in service plan", tenantNotExist)
		log.Error(errMsg)
		response.WriteError(http.StatusBadRequest, errors.New(errMsg))
		return
	}

	log.Info("Check Backends have any buckets before removing")
	for _, backendId := range updateTier.DeleteBackends {
		result, err := s.backendClient.GetBackend(ctx, &backend.GetBackendRequest{Id: backendId})
		if err != nil {
			log.Errorf("failed to get backend details: %v\n", err)
			response.WriteError(http.StatusInternalServerError, err)
			return
		}
		backendname := result.Backend.Name
		res, err := s.s3Client.ListBuckets(ctx, &s3.ListBucketsRequest{Filter: map[string]string{"location": backendname}})
		if err != nil {
			log.Errorf("failed to bucket list details: %v\n", err)
			response.WriteError(http.StatusInternalServerError, err)
			return
		}
		if len(res.Buckets) > 0 {
			errMsg := fmt.Sprintf("failed to update service plan because delete backends: %v have buckets", backendId)
			log.Error(errMsg)
			response.WriteError(http.StatusBadRequest, errors.New(errMsg))
			return
		}
	}

	log.Info("Prepare update list with AddBackends and DeleteBackends")
	res.Tier.Backends = utils.PrepareUpdateList(res.Tier.Backends, updateTier.AddBackends,
		updateTier.DeleteBackends)

	log.Info("Prepare update list with AddTenants and DeleteTenants")
	res.Tier.Tenants = utils.PrepareUpdateList(res.Tier.Tenants, updateTier.AddTenants,
		updateTier.DeleteTenants)

	// Now, tier details can be updated
	updateTierRequest := &backend.UpdateTierRequest{Tier: res.Tier}
	res1, err := s.backendClient.UpdateTier(ctx, updateTierRequest)
	if err != nil {
		log.Errorf("failed to update service plan: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Info("Update service plan successfully.")
	response.WriteEntity(res1.Tier)
}

// GetTier if tierId is given then details of tier to be given
func (s *APIService) GetTier(request *restful.Request, response *restful.Response) {
	log.Infof("Received request for service plan details: %s\n", request.PathParameter("id"))
	if !policy.Authorize(request, response, "tier:get") {
		return
	}

	id := request.PathParameter("id")
	ctx := common.InitCtxWithAuthInfo(request)
	res, err := s.backendClient.GetTier(ctx, &backend.GetTierRequest{Id: id})
	if err != nil {
		log.Errorf("failed to get service plan details: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Info("Get service plan details successfully.")
	response.WriteEntity(res.Tier)
}

//List of tiers is displayed
func (s *APIService) ListTiers(request *restful.Request, response *restful.Response) {
	log.Info("Received request for service plan list.")
	if !policy.Authorize(request, response, "tier:list") {
		return
	}
	ctx := common.GetAdminContext()
	limit, offset, err := common.GetPaginationParam(request)
	if err != nil {
		log.Errorf("get pagination parameters failed: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	var key string
	tenantId := request.PathParameter("tenantId")
	if tenantId == common.DefaultAdminTenantId {
		key = "tenantId"
	} else {
		key = "tenants"
	}

	res, err := s.backendClient.ListTiers(ctx, &backend.ListTierRequest{
		Limit:  limit,
		Offset: offset,
		Filter: map[string]string{key: tenantId},
	})

	if err != nil {
		log.Errorf("failed to list backends: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Info("List service plans successfully.")
	response.WriteEntity(res)
	return
}

//given tierId need to delete the tier
func (s *APIService) DeleteTier(request *restful.Request, response *restful.Response) {
	id := request.PathParameter("id")
	log.Infof("Received request for deleting service plan: %s\n", id)
	if !policy.Authorize(request, response, "tier:delete") {
		return
	}

	ctx := common.InitCtxWithAuthInfo(request)
	res, err := s.backendClient.GetTier(ctx, &backend.GetTierRequest{Id: id})
	if err != nil {
		log.Errorf("failed to get service plan details: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	// check whether tier is empty
	if len(res.Tier.Backends) > 0 {
		log.Errorf("failed to delete service plan because service plan is not empty has backends: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	_, err = s.backendClient.DeleteTier(ctx, &backend.DeleteTierRequest{Id: id})
	if err != nil {
		log.Errorf("failed to delete service plan: %v\n", err)
		err1 := errors.New("failed to delete service plan  because service plan  has associated backends")
		response.WriteError(http.StatusInternalServerError, err1)
		return
	}
	log.Info("Delete service plan successfully.")
	response.WriteHeader(http.StatusOK)
	return
}
