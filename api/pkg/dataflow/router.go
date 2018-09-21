// Copyright (c) 2018 Huawei Technologies Co., Ltd. All Rights Reserved.
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
	ws.Route(ws.GET("/policies/{name}").To(handler.GetPolicy)).
		Doc("Get policy details")
	ws.Route(ws.POST("/policies/{name}").To(handler.CreatePolicy)).
		Doc("Create policy details")

	ws.Route(ws.GET("/policies/{name}").To(handler.GetPolicy)).
		Doc("Get policy details")
	ws.Route(ws.POST("/policies/{name}").To(handler.CreatePolicy)).
		Doc("Create policy")
	ws.Route(ws.PUT("/policies/{id}").To(handler.UpdatePolicy)).
		Doc("Update policy")
	ws.Route(ws.DELETE("/policies/{id}").To(handler.DeletePolicy)).
		Doc("Delete policy")

	ws.Route(ws.GET("/plan/{name}").To(handler.GetPlan)).
		Doc("Get plan details")
	ws.Route(ws.POST("/plan/").To(handler.CreatePlan)).
		Doc("Create plan")
	ws.Route(ws.PUT("/plan/{id}").To(handler.UpdatePlan)).
		Doc("Update plan")
	ws.Route(ws.DELETE("/plan/{id}").To(handler.DeletePlan)).
		Doc("Delete plan")
	ws.Route(ws.POST("/plan/run/{id}").To(handler.RunPlan)).
		Doc("Create connector")
}
