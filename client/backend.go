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

	"github.com/opensds/multi-cloud/backend/proto"
)

// NewBackendMgr implementation
func NewBackendMgr(r Receiver, edp string, tenantID string) *BackendMgr {
	return &BackendMgr{
		Receiver: r,
		Endpoint: edp,
		TenantID: tenantID,
	}
}

// BackendMgr implementation
type BackendMgr struct {
	Receiver
	Endpoint string
	TenantID string
}

// CreateBackend implementation
func (b *BackendMgr) CreateBackend(body *backend.BackendDetail) (*backend.BackendDetail, error) {
	var res backend.BackendDetail
	url := strings.Join([]string{
		b.Endpoint,
		GenerateBackendURL(b.TenantID)}, "/")

	return &res, b.Recv(url, "POST", JSONHeaders, body, &res, true, "")
}

// DeleteBackend implementation
func (b *BackendMgr) DeleteBackend(ID string) error {
	url := strings.Join([]string{
		b.Endpoint,
		GenerateBackendURL(b.TenantID, ID)}, "/")

	return b.Recv(url, "DELETE", JSONHeaders, nil, nil, true, "")
}

// GetBackend implementation
func (b *BackendMgr) GetBackend(ID string) (*backend.BackendDetail, error) {
	var res backend.BackendDetail
	url := strings.Join([]string{
		b.Endpoint,
		GenerateBackendURL(b.TenantID, ID)}, "/")

	if err := b.Recv(url, "GET", JSONHeaders, nil, &res, true, ""); err != nil {
		return nil, err
	}

	return &res, nil
}

// ListBackends implementation
func (b *BackendMgr) ListBackends() (*backend.ListBackendResponse, error) {
	var res backend.ListBackendResponse
	url := strings.Join([]string{
		b.Endpoint,
		GenerateBackendURL(b.TenantID)}, "/")

	if err := b.Recv(url, "GET", JSONHeaders, nil, &res, true, ""); err != nil {
		return nil, err
	}

	return &res, nil
}

// UpdateBackend implementation
func (b *BackendMgr) UpdateBackend(body *backend.UpdateBackendRequest) (*backend.BackendDetail, error) {
	var res backend.BackendDetail
	url := strings.Join([]string{
		b.Endpoint,
		GenerateBackendURL(b.TenantID, body.Id)}, "/")

	if err := b.Recv(url, "PUT", JSONHeaders, body, &res, true, ""); err != nil {
		return nil, err
	}

	return &res, nil
}

// ListTypes List all supported storage backend type
func (b *BackendMgr) ListTypes() (*backend.ListTypeResponse, error) {
	var res backend.ListTypeResponse
	url := strings.Join([]string{
		b.Endpoint,
		GenerateTypeURL(b.TenantID)}, "/")

	if err := b.Recv(url, "GET", JSONHeaders, nil, &res, true, ""); err != nil {
		return nil, err
	}

	return &res, nil
}
