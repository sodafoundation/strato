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

package file

import (
	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/v2/client"
)

//RegisterRouter - route request to appropriate method
func RegisterRouter(ws *restful.WebService) {
	handler := NewAPIService(client.DefaultClient)
	ws.Route(ws.GET("/{tenantId}/file/shares").To(handler.ListFileShare)).
		Doc("List all file shares")
	ws.Route(ws.GET("/{tenantId}/file/shares/{id}").To(handler.GetFileShare)).
		Doc("Show file shares details for a Backend")
	ws.Route(ws.POST("/{tenantId}/file/shares").To(handler.CreateFileShare)).
		Doc("Create file shares")
	ws.Route(ws.PUT("/{tenantId}/file/shares/{id}").To(handler.UpdateFileShare)).
		Doc("Update a file share")
	ws.Route(ws.GET("/{tenantId}/file/shares/{id}/sync").To(handler.SyncFileShare)).
		Doc("Retrieve file share from specified cloud backend and synchronize with DB")
	ws.Route(ws.GET("/{tenantId}/file/shares/sync").To(handler.SyncAllFileShare)).
		Doc("Retrieve all file shares from specified cloud backend and synchronize with DB")
}
