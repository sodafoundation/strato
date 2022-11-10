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

package auth

import (
	"os"

	"github.com/emicklei/go-restful"
	log "github.com/sirupsen/logrus"

	c "github.com/soda/multi-cloud/api/pkg/context"
)

type AuthBase interface {
	Filter(req *restful.Request, resp *restful.Response, chain *restful.FilterChain)
}

type NoAuth struct {
}

func NewNoAuth() AuthBase {
	return &NoAuth{}
}

func (auth *NoAuth) Filter(req *restful.Request, resp *restful.Response, chain *restful.FilterChain) {
	log.Info("Noauth filter begin")
	ctx := req.Attribute(c.KContext).(*c.Context)
	log.Info(ctx.TenantId)
	params := req.PathParameters()
	if tenantId, ok := params["tenantId"]; ok {
		ctx.TenantId = tenantId
	}

	ctx.IsAdmin = ctx.TenantId == c.NoAuthAdminTenantId
	chain.ProcessFilter(req, resp)
}

func FilterFactory() restful.FilterFunction {
	var auth AuthBase
	log.Info(os.Getenv("OS_AUTH_AUTHSTRATEGY"))
	switch os.Getenv("OS_AUTH_AUTHSTRATEGY") {
	case "keystone":
		auth = NewKeystone()
	case "noauth":
		log.Info("filter is noauth")
		auth = NewNoAuth()
	default:
		auth = NewNoAuth()
	}
	return auth.Filter
}
