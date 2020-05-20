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
	//"time"

	"bytes"
	"encoding/xml"
	"github.com/emicklei/go-restful"
	"github.com/opensds/multi-cloud/api/pkg/common"
	"github.com/opensds/multi-cloud/api/pkg/policy"
	block "github.com/opensds/multi-cloud/block/proto"
	log "github.com/sirupsen/logrus"
	"net/http"
	"strconv"
)

func EncodeResponse(response interface{}) []byte {
	var bytesBuffer bytes.Buffer
	bytesBuffer.WriteString(xml.Header)
	e := xml.NewEncoder(&bytesBuffer)
	e.Encode(response)
	return bytesBuffer.Bytes()
}

func WriteSuccessResponse(response *restful.Response, data []byte) {
	if data == nil {
		response.WriteHeader(http.StatusOK)
		return
	}

	response.AddHeader("Content-Length", strconv.Itoa(len(data)))
	response.WriteHeader(http.StatusOK)
	response.Write(data)
	response.Flush()
}

func (s *APIService) ListVolumes(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "volume:list") {
		return
	}
	log.Infof("Received request to list volumes")

	backend := request.PathParameter("backendId")
	ctx := common.InitCtxWithAuthInfo(request)
	rsp, err := s.blockClient.ListVolumes(ctx, &block.VolumeRequest{BackendId: backend})
	if err != nil {
		//TODO: Add proper log messages and log handling
		log.Errorf("List volumes failed with error=%v", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	// write response
	log.Infoln("Listed Volumes successfully\n")
	response.WriteEntity(rsp)
}
