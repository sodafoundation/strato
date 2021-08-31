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

package backend

import (
	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/v2/client"
)

func RegisterRouter(ws *restful.WebService) {
	handler := NewAPIService(client.DefaultClient)
	ws.Route(ws.GET("/{tenantId}/backends/{id}").To(handler.GetBackend)).
		Doc("Show backend details")
	ws.Route(ws.GET("/{tenantId}/backends").To(handler.ListBackend)).
		Doc("Get backend list")
	ws.Route(ws.POST("/{tenantId}/backends").To(handler.CreateBackend)).
		Doc("Create backend")
	ws.Route(ws.PUT("/{tenantId}/backends/{id}").To(handler.UpdateBackend)).
		Doc("Update backend")
	ws.Route(ws.DELETE("/{tenantId}/backends/{id}").To(handler.DeleteBackend)).
		Doc("Delete backend")
	ws.Route(ws.GET("/{tenantId}/types").To(handler.ListType)).
		Doc("Get backend types")
	ws.Route(ws.POST("/{tenantId}/encrypt").To(handler.EncryptData)).
		Doc("Encrypt the data")

	ws.Route(ws.GET("/{tenantId}/ssps/{id}").To(handler.GetSsp)).
		Doc("Show ssp details")
	ws.Route(ws.GET("/{tenantId}/ssps").To(handler.ListSsps)).
		Doc("list ssps")
	ws.Route(ws.POST("/{tenantId}/ssps").To(handler.CreateSsp)).
		Doc("Create ssp")
	ws.Route(ws.PUT("/{tenantId}/ssps/{id}").To(handler.UpdateSsp)).
		Doc("Update ssp")
	ws.Route(ws.DELETE("/{tenantId}/ssps/{id}").To(handler.DeleteSsp)).
		Doc("Delete ssp")

}

