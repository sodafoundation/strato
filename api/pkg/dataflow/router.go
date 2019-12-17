// Copyright 2019 The OpenSDS Authors.
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
	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/client"
)

func RegisterRouter(ws *restful.WebService) {
	handler := NewAPIService(client.DefaultClient)
	ws.Route(ws.POST("/{tenantId}/policies").To(handler.CreatePolicy)).
		Doc("Create policy")
	ws.Route(ws.GET("/{tenantId}/policies").To(handler.ListPolicy)).
		Doc("List policy")
	ws.Route(ws.GET("/{tenantId}/policies/{id}").To(handler.GetPolicy)).
		Doc("Get policy details")
	ws.Route(ws.PUT("/{tenantId}/policies/{id}").To(handler.UpdatePolicy)).
		Doc("Update policy")
	ws.Route(ws.DELETE("/{tenantId}/policies/{id}").To(handler.DeletePolicy)).
		Doc("Delete policy")

	ws.Route(ws.POST("/{tenantId}/plans").To(handler.CreatePlan)).
		Doc("Create plan")
	ws.Route(ws.GET("/{tenantId}/plans").To(handler.ListPlan)).
		Doc("List plan")
	ws.Route(ws.GET("/{tenantId}/plans/{id}").To(handler.GetPlan)).
		Doc("Get plan details")
	ws.Route(ws.PUT("/{tenantId}/plans/{id}").To(handler.UpdatePlan)).
		Doc("Update plan")
	ws.Route(ws.DELETE("/{tenantId}/plans/{id}").To(handler.DeletePlan)).
		Doc("Delete plan")
	ws.Route(ws.POST("/{tenantId}/plans/{id}/run").To(handler.RunPlan)).
		Doc("Create connector")

	ws.Route(ws.GET("/{tenantId}/jobs").To(handler.ListJob)).
		Doc("List jobs details")
	ws.Route(ws.GET("/{tenantId}/jobs/{id}").To(handler.GetJob)).
		Doc("Get job details")
	ws.Route(ws.POST("/{tenantId}/jobs/{id}/abort").To(handler.AbortJob)).
		Doc("Abort migration job")
}
