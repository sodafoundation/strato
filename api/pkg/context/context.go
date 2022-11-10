// Copyright 2019 The soda Authors.
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

// This is self defined context which is stored in context.Input.data.
// It is used to transport data in the pipe line.

package context

import (
	"encoding/json"
	"reflect"

	"github.com/emicklei/go-restful"
	log "github.com/sirupsen/logrus"
)

const (
	KContext = "context"
)

const (
	DefaultTenantId     = "tenantId"
	DefaultUserId       = "userId"
	NoAuthAdminTenantId = "adminTenantId"
)

func NewAdminContext() *Context {
	return &Context{
		TenantId: NoAuthAdminTenantId,
		IsAdmin:  true,
		UserId:   "unknown",
	}
}

func NewContext() *Context {
	return &Context{
		TenantId: DefaultTenantId,
		IsAdmin:  false,
		UserId:   DefaultUserId,
	}
}

func NewContextFromJson(s string) *Context {
	ctx := &Context{}
	err := json.Unmarshal([]byte(s), ctx)
	if err != nil {
		log.Errorf("Unmarshal json to context failed, reason: %v", err)
	}
	return ctx
}

func NewInternalTenantContext(tenantId, userId string, isAdmin bool) *Context {
	return &Context{
		TenantId: tenantId,
		UserId:   userId,
		IsAdmin:  isAdmin,
	}
}

func GetContext(req *restful.Request) *Context {
	ctx, _ := req.Attribute("context").(*Context)
	if ctx == nil {
		ctx = &Context{}
	}
	return ctx
}

type Context struct {
	IsAdmin           bool     `policy:"true" json:"is_admin"`
	AuthToken         string   `policy:"true" json:"auth_token"`
	UserId            string   `policy:"true" json:"user_id"`
	TenantId          string   `policy:"true" json:"tenant_id"`
	DomainId          string   `policy:"true" json:"domain_id"`
	UserDomainId      string   `policy:"true" json:"user_domain_id"`
	ProjectDomainId   string   `policy:"true" json:"project_domain_id"`
	Roles             []string `policy:"true" json:"roles"`
	UserName          string   `policy:"true" json:"user_name"`
	ProjectName       string   `policy:"true" json:"project_name"`
	DomainName        string   `policy:"true" json:"domain_name"`
	UserDomainName    string   `policy:"true" json:"user_domain_name"`
	ProjectDomainName string   `policy:"true" json:"project_domain_name"`
	IsAdminTenant     bool     `policy:"true" json:"is_admin_tenant"`
}

func (ctx *Context) ToPolicyValue() map[string]interface{} {
	ctxMap := map[string]interface{}{}
	t := reflect.TypeOf(ctx).Elem()
	v := reflect.ValueOf(ctx).Elem()

	for i := 0; i < t.NumField(); i++ {
		field := v.Field(i)
		name := t.Field(i).Tag.Get("json")
		if t.Field(i).Tag.Get("policy") == "false" {
			continue
		}
		if field.Kind() == reflect.String && field.String() == "" {
			continue
		}
		if field.Kind() == reflect.Slice && field.Len() == 0 {
			continue
		}
		if field.Kind() == reflect.Map && field.Len() == 0 {
			continue
		}
		ctxMap[name] = field.Interface()
	}
	return ctxMap
}

func (ctx *Context) ToJson() string {
	b, err := json.Marshal(ctx)
	if err != nil {
		log.Errorf("Context convert to json failed, reason: %v", err)
	}
	return string(b)
}
