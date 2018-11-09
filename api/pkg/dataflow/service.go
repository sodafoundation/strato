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

package dataflow

import (
	"encoding/json"
	"net/http"

	"io/ioutil"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"
	"github.com/micro/go-micro/client"
	"github.com/opensds/multi-cloud/api/pkg/common"
	c "github.com/opensds/multi-cloud/api/pkg/filters/context"
	"github.com/opensds/multi-cloud/api/pkg/policy"
	"github.com/opensds/multi-cloud/backend/proto"
	"github.com/opensds/multi-cloud/dataflow/proto"
	"github.com/opensds/multi-cloud/s3/proto"
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

func (s *APIService) ListPolicy(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "policy:list") {
		return
	}
	actx := request.Attribute(c.KContext).(*c.Context)

	ctx := context.Background()
	res, err := s.dataflowClient.ListPolicy(ctx, &dataflow.ListPolicyRequest{Context: actx.ToJson()})
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	//For debug -- begin
	log.Logf("Get policy reponse:%v", res)
	jsons, errs := json.Marshal(res)
	if errs != nil {
		log.Logf(errs.Error())
	} else {
		log.Logf("res: %s.\n", jsons)
	}
	//For debug -- end

	log.Log("List policy details successfully.")
	response.WriteEntity(res)
}

func (s *APIService) GetPolicy(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "policy:get") {
		return
	}
	actx := request.Attribute(c.KContext).(*c.Context)
	id := request.PathParameter("id")
	log.Logf("Received request for policy[id=%s] details.", id)
	ctx := context.Background()
	res, err := s.dataflowClient.GetPolicy(ctx, &dataflow.GetPolicyRequest{Context: actx.ToJson(), Id: id})
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	//For debug -- begin
	log.Logf("Get policy reponse:%v", res)
	jsons, errs := json.Marshal(res)
	if errs != nil {
		log.Logf(errs.Error())
	} else {
		log.Logf("res: %s.\n", jsons)
	}
	//For debug -- end

	log.Log("Get policy details successfully.")
	response.WriteEntity(res)
}

func (s *APIService) CreatePolicy(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "policy:create") {
		return
	}
	actx := request.Attribute(c.KContext).(*c.Context)
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
	} else {
		log.Logf("Req body: %s.\n", jsons)
	}
	//For debug --end

	res, err := s.dataflowClient.CreatePolicy(ctx, &dataflow.CreatePolicyRequest{Context: actx.ToJson(), Policy: &pol})
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Log("Get policy details successfully.")
	response.WriteEntity(res)
}

func (s *APIService) UpdatePolicy(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "policy:update") {
		return
	}
	actx := request.Attribute(c.KContext).(*c.Context)
	policyId := request.PathParameter("id")
	body, err := ioutil.ReadAll(request.Request.Body)
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		log.Logf("Read request body failed, err:%v.\n", err)
		return
	}
	log.Logf("Received request for update policy.body:%s\n", body)

	ctx := context.Background()
	req := &dataflow.UpdatePolicyRequest{Context: actx.ToJson(), PolicyId: policyId, Body: string(body)}
	res, err := s.dataflowClient.UpdatePolicy(ctx, req)
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	response.WriteEntity(res)

	//For debug --begin
	jsons1, errs1 := json.Marshal(res)
	if errs1 != nil {
		log.Logf(errs1.Error())
	} else {
		log.Logf("Rsp body: %s.\n", jsons1)
	}
	//For debug --end
	log.Log("Update policy successfully.")
}

func (s *APIService) DeletePolicy(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "policy:delete") {
		return
	}
	actx := request.Attribute(c.KContext).(*c.Context)
	id := request.PathParameter("id")
	log.Logf("Received request for delete policy[id=%s] details.", id)
	ctx := context.Background()
	res, err := s.dataflowClient.DeletePolicy(ctx, &dataflow.DeletePolicyRequest{Context: actx.ToJson(), Id: id})
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Log("Delete policy end, err = %d.", err)
	response.WriteEntity(res)
}

func (s *APIService) ListPlan(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "plan:list") {
		return
	}
	log.Log("Received request for list plan.")

	listPlanReq := &dataflow.ListPlanRequest{}
	limit, offset, err := common.GetPaginationParam(request)
	if err != nil {
		log.Logf("Get pagination parameters failed: %v", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	listPlanReq.Limit = limit
	listPlanReq.Offset = offset

	sortKeys, sortDirs, err := common.GetSortParam(request)
	if err != nil {
		log.Logf("Get sort parameters failed: %v", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	listPlanReq.SortKeys = sortKeys
	listPlanReq.SortDirs = sortDirs

	filterOpts := []string{"name", "type", "bucketname"}
	filter, err := common.GetFilter(request, filterOpts)
	if err != nil {
		log.Logf("Get filter failed: %v", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	} else {
		log.Logf("Get filter for list plan: %v", filter)
	}
	listPlanReq.Filter = filter

	actx := request.Attribute(c.KContext).(*c.Context)
	listPlanReq.Context = actx.ToJson()

	ctx := context.Background()
	res, err := s.dataflowClient.ListPlan(ctx, listPlanReq)
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	//For debug -- begin
	log.Logf("List plan reponse:%v", res)
	jsons, errs := json.Marshal(res)
	if errs != nil {
		log.Logf(errs.Error())
	} else {
		log.Logf("res: %s.\n", jsons)
	}
	//For debug -- end

	log.Log("Get plan details successfully.")
	response.WriteEntity(res)
}

func (s *APIService) GetPlan(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "plan:get") {
		return
	}
	actx := request.Attribute(c.KContext).(*c.Context)
	id := request.PathParameter("id")
	log.Logf("Received request for plan[id=%s] details.", id)
	ctx := context.Background()
	res, err := s.dataflowClient.GetPlan(ctx, &dataflow.GetPlanRequest{Context: actx.ToJson(), Id: id})
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	//For debug -- begin
	log.Logf("Get plan reponse:%v", res)
	jsons, errs := json.Marshal(res)
	if errs != nil {
		log.Logf(errs.Error())
	} else {
		log.Logf("res: %s.\n", jsons)
	}
	//For debug -- end

	log.Log("Get plan details successfully.")
	response.WriteEntity(res)
}

func (s *APIService) CreatePlan(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "plan:create") {
		return
	}
	//name := request.PathParameter("name")
	actx := request.Attribute(c.KContext).(*c.Context)
	log.Logf("Received request for create plan.\n")
	ctx := context.Background()
	plan := dataflow.Plan{}
	err := request.ReadEntity(&plan)
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		log.Logf("Read request body failed, err:%v.\n", err)
		return
	}

	//For debug --begin
	jsons, errs := json.Marshal(plan)
	if errs != nil {
		log.Logf(errs.Error())
	} else {
		log.Logf("Req body: %s.\n", jsons)
	}
	//For debug --end

	resp, err := s.dataflowClient.CreatePlan(ctx, &dataflow.CreatePlanRequest{Context: actx.ToJson(), Plan: &plan})
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Log("Create plan successfully.")
	response.WriteEntity(resp)
}

func (s *APIService) UpdatePlan(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "plan:update") {
		return
	}
	actx := request.Attribute(c.KContext).(*c.Context)

	planId := request.PathParameter("id")
	log.Logf("Received request for update plan(%d).\n", planId)
	body, err := ioutil.ReadAll(request.Request.Body)
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		log.Logf("Read request body failed, err:%v.\n", err)
		return
	}
	log.Logf("Req body: %s.\n", string(body))

	ctx := context.Background()
	req := &dataflow.UpdatePlanRequest{Context: actx.ToJson(), PlanId: planId, Body: string(body)}
	resp, err := s.dataflowClient.UpdatePlan(ctx, req)
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Log("Update plan successfully.")
	response.WriteEntity(resp)

	//For debug --begin
	jsons1, errs1 := json.Marshal(resp)
	if errs1 != nil {
		log.Logf(errs1.Error())
	} else {
		log.Logf("Rsp body: %s.\n", jsons1)
	}
	//For debug --end
}

func (s *APIService) DeletePlan(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "plan:delete") {
		return
	}
	actx := request.Attribute(c.KContext).(*c.Context)
	id := request.PathParameter("id")
	log.Logf("Received request for delete plan[id=%s] details.", id)
	ctx := context.Background()
	res, err := s.dataflowClient.DeletePlan(ctx, &dataflow.DeletePlanRequest{Context: actx.ToJson(), Id: id})
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	response.WriteEntity(res)
}

func (s *APIService) RunPlan(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "plan:run") {
		return
	}
	actx := request.Attribute(c.KContext).(*c.Context)
	id := request.PathParameter("id")
	log.Logf("Received request for run plan[id=%s] details.", id)
	ctx := context.Background()
	res, err := s.dataflowClient.RunPlan(ctx, &dataflow.RunPlanRequest{Context: actx.ToJson(), Id: id})
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	response.WriteEntity(res)
}

func (s *APIService) GetJob(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "job:get") {
		return
	}
	actx := request.Attribute(c.KContext).(*c.Context)
	id := request.PathParameter("id")
	log.Logf("Received request jobs [id=%s] details.", id)
	ctx := context.Background()
	res, err := s.dataflowClient.GetJob(ctx, &dataflow.GetJobRequest{Context: actx.ToJson(), Id: id})
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	//For debug -- begin
	log.Logf("Get jobs reponse:%v", res)
	jsons, errs := json.Marshal(res)
	if errs != nil {
		log.Logf(errs.Error())
	} else {
		log.Logf("res: %s.\n", jsons)
	}
	//For debug -- end

	log.Log("Get job details successfully.")
	response.WriteEntity(res)
}

func (s *APIService) ListJob(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "job:list") {
		return
	}
	log.Log("Received request for list jobs.")

	listJobReq := &dataflow.ListJobRequest{}
	limit, offset, err := common.GetPaginationParam(request)
	if err != nil {
		log.Logf("Get pagination parameters failed: %v", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	listJobReq.Limit = limit
	listJobReq.Offset = offset

	sortKeys, sortDirs, err := common.GetSortParam(request)
	if err != nil {
		log.Logf("Get sort parameters failed: %v", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	listJobReq.SortKeys = sortKeys
	listJobReq.SortDirs = sortDirs

	filterOpts := []string{"planname", "type"}
	filter, err := common.GetFilter(request, filterOpts)
	if err != nil {
		log.Logf("Get filter failed: %v", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	listJobReq.Filter = filter

	actx := request.Attribute(c.KContext).(*c.Context)
	listJobReq.Context = actx.ToJson()

	ctx := context.Background()
	res, err := s.dataflowClient.ListJob(ctx, listJobReq)
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	//For debug -- begin
	log.Logf("Get jobs reponse:%v", res)
	jsons, errs := json.Marshal(res)
	if errs != nil {
		log.Logf(errs.Error())
	} else {
		log.Logf("res: %s.\n", jsons)
	}
	//For debug -- end

	log.Log("Get plan details successfully.")
	response.WriteEntity(res)
}
