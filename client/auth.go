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
	"log"
	"strings"
)

const (
	// Keystone value of OS_AUTH_AUTHSTRATEGY
	Keystone = "keystone"
	// Noauth value of OS_AUTH_AUTHSTRATEGY
	Noauth = "noauth"

	// MicroServerAddress Api environment variable name in docker-compose.yml
	MicroServerAddress = "MICRO_SERVER_ADDRESS"
	// OsAuthAuthstrategy Api environment variable name in docker-compose.yml
	OsAuthAuthstrategy = "OS_AUTH_AUTHSTRATEGY"
	// OsAuthURL Api environment variable name in docker-compose.yml
	OsAuthURL = "OS_AUTH_URL"
	// OsUserName Api environment variable name in docker-compose.yml
	OsUserName = "OS_USERNAME"
	// OsPassword Api environment variable name in docker-compose.yml
	OsPassword = "OS_PASSWORD"
	// OsTenantName Api environment variable name in docker-compose.yml
	OsTenantName = "OS_TENANT_NAME"
	// OsProjectName Api environment variable name in docker-compose.yml
	OsProjectName = "OS_PROJECT_NAME"
	// OsUserDominID Api environment variable name in docker-compose.yml
	OsUserDominID = "OS_USER_DOMIN_ID"
)

// AuthOptions Auth Options
type AuthOptions interface {
	GetTenantID() string
}

// NewKeystoneAuthOptions implementation
func NewKeystoneAuthOptions() *KeystoneAuthOptions {
	return &KeystoneAuthOptions{}
}

// KeystoneAuthOptions implementation
type KeystoneAuthOptions struct {
	IdentityEndpoint string
	Username         string
	UserID           string
	Password         string
	DomainID         string
	DomainName       string
	TenantID         string
	TenantName       string
	AllowReauth      bool
	TokenID          string
}

// GetTenantID Get TenantId
func (k *KeystoneAuthOptions) GetTenantID() string {
	return k.TenantID
}

// NewNoauthOptions implementation
func NewNoauthOptions(tenantID string) *NoAuthOptions {
	return &NoAuthOptions{TenantID: tenantID}
}

// NoAuthOptions implementation
type NoAuthOptions struct {
	TenantID string
}

// GetTenantID implementation
func (n *NoAuthOptions) GetTenantID() string {
	return n.TenantID
}

// GetValueFromStrArray implementation
func GetValueFromStrArray(strArray []string, key string) string {
	value := ""

	for _, str := range strArray {
		if strings.HasPrefix(str, key+"=") {
			authArray := strings.Split(str, "=")

			if len(authArray) > 1 {
				value = authArray[1]
			} else {
				log.Printf("There is no value in %+v ", key)
			}

			break
		}
	}

	log.Printf("There is no %+v in %+v ", key, strArray)
	return value
}

// LoadKeystoneAuthOptions implementation
func LoadKeystoneAuthOptions(envs []string) *KeystoneAuthOptions {
	opt := NewKeystoneAuthOptions()
	opt.IdentityEndpoint = GetValueFromStrArray(envs, OsAuthURL)
	opt.Username = GetValueFromStrArray(envs, OsUserName)
	opt.Password = GetValueFromStrArray(envs, OsPassword)
	opt.TenantName = GetValueFromStrArray(envs, OsTenantName)
	projectName := GetValueFromStrArray(envs, OsProjectName)
	opt.DomainID = GetValueFromStrArray(envs, OsUserDominID)
	if opt.TenantName == "" {
		opt.TenantName = projectName
	}

	return opt
}
