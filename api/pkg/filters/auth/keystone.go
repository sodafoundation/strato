// Copyright 2019 The OpenSDS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

// Keystone authentication middleware, only support keystone v3.
package auth

import (
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/emicklei/go-restful"
	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/openstack"
	"github.com/gophercloud/gophercloud/openstack/identity/v3/tokens"
	log "github.com/sirupsen/logrus"

	c "github.com/opensds/multi-cloud/api/pkg/context"
	"github.com/opensds/multi-cloud/api/pkg/model"
	"github.com/opensds/multi-cloud/api/pkg/utils"
	"github.com/opensds/multi-cloud/api/pkg/utils/constants"
)

type Keystone struct {
	identity *gophercloud.ServiceClient
}

func GetIdentity(k *Keystone) *gophercloud.ServiceClient {
	return k.identity
}

func NewKeystone() AuthBase {
	k := &Keystone{}
	if err := k.SetUp(); err != nil {
		// If auth set up failed, raise panic.
		panic(err)
	}
	return k
}

func (k *Keystone) SetUp() error {
	opts := gophercloud.AuthOptions{
		IdentityEndpoint: os.Getenv("OS_AUTH_URL"),
		DomainName:       os.Getenv("OS_USER_DOMIN_ID"),
		Username:         os.Getenv("OS_USERNAME"),
		Password:         os.Getenv("OS_PASSWORD"),
		TenantName:       os.Getenv("OS_PROJECT_NAME"),
	}
	log.Infof("opts:%v", opts)
	provider, err := openstack.AuthenticatedClient(opts)
	if err != nil {
		log.Errorf("When get auth client:%s", err)
		return err
	}
	// Only support keystone v3
	k.identity, err = openstack.NewIdentityV3(provider, gophercloud.EndpointOpts{})
	if err != nil {
		log.Errorf("When get identity session: %s", err)
		return err
	}
	log.Infof("Service Token Info: %s", provider.TokenID)
	return nil
}

func (k *Keystone) Filter(req *restful.Request, resp *restful.Response, chain *restful.FilterChain) {
	// Strip the spaces around the token  ctx.Input.Header(constants.AuthTokenHeader)
	token := strings.TrimSpace(req.HeaderParameter(constants.AuthTokenHeader))
	if err := k.validateToken(req, resp, token); err != nil {
		return
	}
	chain.ProcessFilter(req, resp)
}

func (k *Keystone) validateToken(req *restful.Request, res *restful.Response, token string) error {
	if token == "" {
		return model.HttpError(res, http.StatusUnauthorized, "token not found in header")
	}
	var r tokens.GetResult
	// The service token may be expired or revoked, so retry to get new token.
	err := utils.Retry(2, "verify token", false, func(retryIdx int, lastErr error) error {
		if retryIdx > 0 {
			// Fixme: Is there any better method ?
			if lastErr.Error() == "Authentication failed" {
				k.SetUp()
			} else {
				return lastErr
			}
		}
		log.Infof("k.identity: %v", k.identity)
		r = tokens.Get(k.identity, token)
		log.Infof("r: %v", r)
		log.Infof("r.err: %s", r.Err)
		return r.Err
	})
	if err != nil {
		return model.HttpError(res, http.StatusUnauthorized, "get token failed,%v", r.Err)
	}

	t, err := r.ExtractToken()
	if err != nil {
		return model.HttpError(res, http.StatusUnauthorized, "extract token failed,%v", err)

	}
	log.Infof("token: %v", t)

	if time.Now().After(t.ExpiresAt) {
		return model.HttpError(res, http.StatusUnauthorized,
			"token has expired, expire time %v", t.ExpiresAt)
	}
	return k.setPolicyContext(req, res, r)
}

func (k *Keystone) setPolicyContext(req *restful.Request, res *restful.Response, r tokens.GetResult) error {
	roles, err := r.ExtractRoles()
	if err != nil {
		return model.HttpError(res, http.StatusUnauthorized, "extract roles failed,%v", err)
	}

	var roleNames []string
	for _, role := range roles {
		roleNames = append(roleNames, role.Name)
	}

	project, err := r.ExtractProject()
	if err != nil {
		return model.HttpError(res, http.StatusUnauthorized, "extract project failed,%v", err)
	}

	user, err := r.ExtractUser()
	if err != nil {
		return model.HttpError(res, http.StatusUnauthorized, "extract user failed,%v", err)
	}

	t, _ := r.ExtractToken()
	ctx := req.Attribute(c.KContext).(*c.Context)
	ctx.AuthToken = t.ID
	ctx.TenantId = project.ID
	ctx.UserId = user.ID
	ctx.Roles = roleNames
	ctx.IsAdminTenant = strings.ToLower(project.Name) == "admin"
	ctx.IsAdmin = ctx.IsAdminTenant
	return nil
}
