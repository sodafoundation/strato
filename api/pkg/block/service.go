// Copyright 2020 The OpenSDS Authors.
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
	"github.com/micro/go-micro/client"
	"github.com/opensds/multi-cloud/api/pkg/policy"
	"github.com/opensds/multi-cloud/backend/proto"
	"github.com/opensds/multi-cloud/block/proto"
)

const (
	blockService   = "block"
	backendService = "backend"
)

type APIService struct {
	blockClient   block.BlockService
	backendClient backend.BackendService
}

func NewAPIService(c client.Client) *APIService {
	return &APIService{
		blockClient:   block.NewBlockService(blockService, c),
		backendClient: backend.NewBackendService(backendService, c),
	}
}

func (s *APIService) RouteVolumeList(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "volume:list") {
		return
	}

	s.ListVolumes(request, response)
}
