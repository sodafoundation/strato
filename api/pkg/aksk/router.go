// Copyright 2021 The soda Authors.
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

package aksk

import (
	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/v2/client"
)

func RegisterRouter(ws *restful.WebService) {
	handler := NewAPIService(client.DefaultClient)
	ws.Route(ws.GET("/{tenantId}/aksks/{id}").To(handler.GetAkSk)).Doc("Show AkSk details")
	ws.Route(ws.GET("/{tenantId}/aksks/{id}/download").To(handler.DownloadAkSk)).Doc("Download AkSk details")
	ws.Route(ws.POST("/{tenantId}/aksks").To(handler.CreateAkSk)).Doc("Create AkSk")
	ws.Route(ws.DELETE("/{tenantId}/aksks/{id}").To(handler.DeleteAkSk)).Doc("Delete AkSk")

}
