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
	"github.com/opensds/multi-cloud/api/pkg/common"
	"github.com/opensds/multi-cloud/api/pkg/policy"
	block "github.com/opensds/multi-cloud/block/proto"
	log "github.com/sirupsen/logrus"
	"net/http"
)

func (s *APIService) ListVolumes(request *restful.Request, response *restful.Response) {
	log.Infof("Received request to list volumes")

	backend := request.PathParameter("backendId")
	ctx := common.InitCtxWithAuthInfo(request)
	rsp, err := s.blockClient.ListVolumes(ctx, &block.VolumeRequest{BackendId: backend})
	log.Infof("Volume resp is rsp =[%v]\n", rsp)
	if err != nil {
		log.Errorf("List volumes failed with error=%v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	// write response
	log.Infoln("Listed Volumes successfully\n")
	response.WriteEntity(rsp)
}
