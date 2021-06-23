// Copyright 2021 The SODA Foundation Authors.
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

package model

//AkSk is used to hold the data to generate the AccessKey and SecretKey for the User.

type AkSk struct {

	// ProjectId or TenantId is the tenant that the user belongs to.
	ProjectId string `json:"project_id,omitempty"`

	// The id of the user for whom the AkSk is being generated.
	UserId string `json:"user_id,omitempty"`

	// The json containing the accesskey and secretkey
	Blob string `json:"blob,omitempty"`

	//The type of backend ??
	Type string `json:"type,omitempty"`
}

type GetAkSkBody struct {
	// ProjectId or TenantId is the tenant that the user belongs to.
	ProjectId string `json:"project_id,omitempty"`

	// The id of the user for whom the AkSk is being generated.
	UserId string `json:"user_id,omitempty"`

	// The json containing the accesskey and secretkey
	Blob string `json:"blob,omitempty"`

	//The type of backend
	Type string `json:"type,omitempty"`

	Links string `json:"links,omitempty"`
}

type GetAkSk struct {
	Credentials GetAkSkBody `json:"credentials,omitempty"`
}

type AkSkBlob struct {
	Blob string `json:"aksk,omitempty"`
}

type AkSkOut struct {
	Credential struct {
		UserID string `json:"user_id"`
		Links  struct {
			Self string `json:"self"`
		} `json:"links"`
		Blob      string `json:"blob"`
		ProjectID string `json:"project_id"`
		Type      string `json:"type"`
		ID        string `json:"id"`
	} `json:"credential"`
}

type AkSkListOut struct {
	Credentials []struct {
		UserID string `json:"user_id"`
		Links  struct {
			Self string `json:"self"`
		} `json:"links"`
		Blob      string `json:"blob"`
		ProjectID string `json:"project_id"`
		Type      string `json:"type"`
		ID        string `json:"id"`
	} `json:"credentials"`
	Links struct {
		Self     string      `json:"self"`
		Previous interface{} `json:"previous"`
		Next     interface{} `json:"next"`
	} `json:"links"`
}
