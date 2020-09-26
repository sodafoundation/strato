// Copyright 2020 The SODA Authors.
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

package block

import (
	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/v2/client"
)

//RegisterRouter - route request to appropriate method
func RegisterRouter(ws *restful.WebService) {
	handler := NewAPIService(client.DefaultClient)
	ws.Route(ws.GET("/{tenantId}/volumes").To(handler.ListVolumes)).
		Doc("List all block devices")
	ws.Route(ws.GET("/{tenantId}/volumes/{id}").To(handler.GetVolume)).
		Doc("Show Volume details for a Backend")
	ws.Route(ws.POST("/{tenantId}/volumes").To(handler.CreateVolume)).
		Doc("Create Volume")
	ws.Route(ws.PUT("/{tenantId}/volumes/{id}").To(handler.UpdateVolume)).
		Doc("Update a Volume")
	ws.Route(ws.DELETE("/{tenantId}/volumes/{id}").To(handler.DeleteVolume)).
		Doc("Delete Volume")

}
