// Copyright 2019 The soda Authors.
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

// Package keystonecredentials provides support for retrieving credentials from keystone

package keystonecredentials

import (
	"encoding/json"
	"strings"

	"github.com/gophercloud/gophercloud"
	creds "github.com/gophercloud/gophercloud/openstack/identity/v3/credentials"
	log "github.com/sirupsen/logrus"

	"github.com/soda/multi-cloud/api/pkg/filters/auth"
	"github.com/soda/multi-cloud/api/pkg/filters/signature/credentials"
	"github.com/soda/multi-cloud/api/pkg/model"
)

// ProviderName is the name of the credentials provider.
const ProviderName = `KeystoneProvider`

// KeystoneProvider is a client to retrieve credentials from Keystone.
type KeystoneProvider struct {
	// Requires a gopher cloud Client to make HTTP requests to the Keystone with.
	Identity *gophercloud.ServiceClient

	// Requires an AccessKeyID to filter the credentials request.
	AccessKeyID string
}

type Blob struct {
	Access string `json:"access"`
	Secret string `json:"secret"`
}

// NewProviderClient returns a credentials Provider for retrieving credentials
func NewProviderClient(accessKeyID string, options ...func(*KeystoneProvider)) credentials.Provider {

	k := &auth.Keystone{}
	if err := k.SetUp(); err != nil {
		// If auth set up failed, raise panic.
		panic(err)
	}

	kp := &KeystoneProvider{
		AccessKeyID: accessKeyID,
	}
	kp.Identity = auth.GetIdentity(k)

	log.Infof("Service Token Info: %s", kp.Identity.TokenID)

	return kp
}

// NewCredentialsClient returns a Credentials wrapper for retrieving credentials
func NewCredentialsClient(accessKeyID string, options ...func(*KeystoneProvider)) *credentials.Credentials {
	return credentials.NewCredentials(NewProviderClient(accessKeyID, options...))
}

// Retrieve will attempt to request the credentials from the Keystone
// And error will be returned if the retrieval fails.
func (p *KeystoneProvider) Retrieve() (credentials.Value, error) {
	resp, err := p.getCredentials(p.AccessKeyID)
	if err != nil {
		return credentials.Value{ProviderName: ProviderName}, err
	}

	return credentials.Value{
		AccessKeyID:     resp.AccessKeyID,
		SecretAccessKey: resp.SecretAccessKey,
		TenantID:        resp.TenantID,
		UserID:          resp.UserID,
		ProviderName:    ProviderName,
	}, nil
}

type GetCredentialOutput struct {
	AccessKeyID     string
	SecretAccessKey string
	TenantID        string
	UserID          string
}

// Returns AccessKey and SecretKey Values, Retrieves Credentials
// from Keystone And error will be returned if the retrieval fails.
func (p *KeystoneProvider) getCredentials(accessKeyID string) (*GetCredentialOutput, error) {

	allPages, err := creds.List(p.Identity, nil).AllPages()

	credentials, err := creds.ExtractCredentials(allPages)
	if err != nil {
		log.Errorf("getCredentials failed, err: %+v", err)
		return nil, err
	}
	log.Infof("provider-Credentials: %+v", credentials)

	cred, err := getCredential(credentials, accessKeyID)
	log.Infof("cred: %+v", cred)

	return cred, err
}

// Returns a credential Blob for getting access and secret
// And error will be returned if it fails.
func getCredential(credentials []creds.Credential, accessKeyID string) (*GetCredentialOutput, error) {
	blob := &Blob{}
	for _, credential := range credentials {
		var blobStr = credential.Blob
		b := strings.Replace(blobStr, "\\", "", -1)
		err := json.Unmarshal([]byte(b), blob)

		if err != nil {
			return nil, err
		}
		if blob.Access == accessKeyID {
			out := &GetCredentialOutput{
				AccessKeyID:     blob.Access,
				SecretAccessKey: blob.Secret,
				TenantID:        credential.ProjectID,
				UserID:          credential.UserID,
			}

			log.Infof("Get credential for %s successfully.", blob.Access)
			return out, nil
		}
	}
	return nil, model.NewNotFoundError("credential is missing")
}
