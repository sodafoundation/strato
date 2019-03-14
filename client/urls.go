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

	"github.com/opensds/multi-cloud/api/pkg/utils/obs"
)

var (
	// JSONHeaders "content-type": "application/json"
	JSONHeaders = HeaderOption{obs.HEADER_CONTENT_TYPE: "application/json"}
	// XMLHeaders "content-type": "application/xml"
	XMLHeaders = HeaderOption{obs.HEADER_CONTENT_TYPE: "application/xml"}
)

// GenerateBackendURL implementation
func GenerateBackendURL(tenantID string, in ...string) string {
	return generateURL("backends", tenantID, in...)
}

// GenerateTypeURL implementation
func GenerateTypeURL(tenantID string, in ...string) string {
	return generateURL("types", tenantID, in...)
}

// GeneratePlanURL implementation
func GeneratePlanURL(tenantID string, in ...string) string {
	return generateURL("plans", tenantID, in...)
}

// GenerateJobURL implementation
func GenerateJobURL(tenantID string, in ...string) string {
	return generateURL("jobs", tenantID, in...)
}

// GeneratePolicyURL implementation
func GeneratePolicyURL(tenantID string, in ...string) string {
	return generateURL("policies", tenantID, in...)
}

// GenerateS3URL implementation
func GenerateS3URL(tenantID string, in ...string) string {
	return generateURL("s3", "", in...)
}

// CurrentVersion implementation
func CurrentVersion() string {
	return "v1"
}

func generateURL(resource string, tenantID string, in ...string) string {
	// If project id is not specified, ignore it.
	if tenantID == "" {
		value := []string{CurrentVersion(), resource}
		value = append(value, in...)
		return strings.Join(value, "/")
	}

	value := []string{CurrentVersion(), tenantID, resource}
	value = append(value, in...)

	return strings.Join(value, "/")
}
