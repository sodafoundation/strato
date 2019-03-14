// Copyright (c) 2019 Huawei Technologies Co., Ltd. All Rights Reserved.
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

package client

import (
	"strings"

	"github.com/opensds/multi-cloud/dataflow/proto"
)

// NewDataflowMgr implementation
func NewDataflowMgr(r Receiver, edp string, tenantID string) *DataflowMgr {
	return &DataflowMgr{
		Receiver: r,
		Endpoint: edp,
		TenantID: tenantID,
	}
}

// DataflowMgr implementation
type DataflowMgr struct {
	Receiver
	Endpoint string
	TenantID string
}

// CreatePlan implementation
func (b *BackendMgr) CreatePlan(body *dataflow.Plan) (*dataflow.Plan, error) {
	var res dataflow.CreatePlanResponse
	url := strings.Join([]string{
		b.Endpoint,
		GeneratePlanURL(b.TenantID)}, "/")

	if err := b.Recv(url, "POST", JSONHeaders, body, &res, true, ""); err != nil {
		return nil, err
	}

	return res.Plan, nil
}

// ListPlan implementation
func (b *BackendMgr) ListPlan() ([]*dataflow.Plan, error) {
	var res dataflow.ListPlanResponse
	url := strings.Join([]string{
		b.Endpoint,
		GeneratePlanURL(b.TenantID)}, "/")

	if err := b.Recv(url, "GET", JSONHeaders, nil, &res, false, ""); err != nil {
		return nil, err
	}

	return res.Plans, nil
}

// ShowPlan implementation
func (b *BackendMgr) ShowPlan(id string) (*dataflow.Plan, error) {
	var res dataflow.GetPlanResponse
	url := strings.Join([]string{
		b.Endpoint,
		GeneratePlanURL(b.TenantID), id}, "/")

	if err := b.Recv(url, "GET", JSONHeaders, nil, &res, false, ""); err != nil {
		return nil, err
	}

	return res.Plan, nil
}

// UpdatePlan implementation
func (b *BackendMgr) UpdatePlan(id, body string) (*dataflow.Plan, error) {
	var res dataflow.UpdatePlanResponse
	url := strings.Join([]string{
		b.Endpoint,
		GeneratePlanURL(b.TenantID), id}, "/")

	if err := b.Recv(url, "PUT", JSONHeaders, body, &res, false, ""); err != nil {
		return nil, err
	}

	return res.Plan, nil
}

// DeletePlan implementation
func (b *BackendMgr) DeletePlan(id string) error {
	url := strings.Join([]string{
		b.Endpoint,
		GeneratePlanURL(b.TenantID), id}, "/")

	return b.Recv(url, "DELETE", JSONHeaders, nil, nil, false, "")
}

// RunPlan implementation
func (b *BackendMgr) RunPlan(id string) (*dataflow.RunPlanResponse, error) {
	var res dataflow.RunPlanResponse
	url := strings.Join([]string{
		b.Endpoint,
		GeneratePlanURL(b.TenantID), id, "run"}, "/")

	if err := b.Recv(url, "POST", JSONHeaders, nil, &res, false, ""); err != nil {
		return &res, err
	}

	return &res, nil
}

// CreatePolicy implementation
func (b *BackendMgr) CreatePolicy(body *dataflow.Policy) (*dataflow.Policy, error) {
	var res dataflow.CreatePolicyResponse
	url := strings.Join([]string{
		b.Endpoint,
		GeneratePolicyURL(b.TenantID)}, "/")

	if err := b.Recv(url, "POST", JSONHeaders, body, &res, true, ""); err != nil {
		return nil, err
	}

	return res.Policy, nil
}

// ShowPolicy implementation
func (b *BackendMgr) ShowPolicy(id string) (*dataflow.Policy, error) {
	var res dataflow.CreatePolicyResponse
	url := strings.Join([]string{
		b.Endpoint,
		GeneratePolicyURL(b.TenantID), id}, "/")

	if err := b.Recv(url, "GET", JSONHeaders, nil, &res, true, ""); err != nil {
		return nil, err
	}

	return res.Policy, nil
}

// ListPolicy implementation
func (b *BackendMgr) ListPolicy() ([]*dataflow.Policy, error) {
	var res dataflow.ListPolicyResponse
	url := strings.Join([]string{
		b.Endpoint,
		GeneratePolicyURL(b.TenantID)}, "/")

	if err := b.Recv(url, "GET", JSONHeaders, nil, &res, true, ""); err != nil {
		return nil, err
	}

	return res.Policies, nil
}

// UpdatePolicy implementation
func (b *BackendMgr) UpdatePolicy(id, body string) (*dataflow.Policy, error) {
	var res dataflow.UpdatePolicyResponse
	url := strings.Join([]string{
		b.Endpoint,
		GeneratePolicyURL(b.TenantID), id}, "/")

	if err := b.Recv(url, "PUT", JSONHeaders, body, &res, false, ""); err != nil {
		return nil, err
	}

	return res.Policy, nil
}

// DeletePolicy implementation
func (b *BackendMgr) DeletePolicy(id string) error {
	url := strings.Join([]string{
		b.Endpoint,
		GeneratePolicyURL(b.TenantID), id}, "/")

	return b.Recv(url, "DELETE", JSONHeaders, nil, nil, false, "")
}

// ListJob implementation
func (b *BackendMgr) ListJob() ([]*dataflow.Job, error) {
	var res dataflow.ListJobResponse
	url := strings.Join([]string{
		b.Endpoint,
		GenerateJobURL(b.TenantID)}, "/")

	if err := b.Recv(url, "GET", JSONHeaders, nil, &res, true, ""); err != nil {
		return nil, err
	}

	return res.Jobs, nil
}

// ShowJob implementation
func (b *BackendMgr) ShowJob(ID string) (*dataflow.Job, error) {
	var res dataflow.GetJobResponse

	url := strings.Join([]string{
		b.Endpoint,
		GenerateJobURL(b.TenantID), ID}, "/")

	if err := b.Recv(url, "GET", JSONHeaders, nil, &res, true, ""); err != nil {
		return nil, err
	}

	return res.Job, nil
}
