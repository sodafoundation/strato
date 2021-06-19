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

package akskPackage

import (
	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/v2/client"
	"github.com/opensds/multi-cloud/aksk/proto"
	"github.com/opensds/multi-cloud/api/pkg/policy"
	log "github.com/sirupsen/logrus"
)

const (
	akskService = "aksk"
)

type APIService struct {
	akskClient aksk.AkSkService
}

func NewAPIService(c client.Client) *APIService {
	return &APIService{
		akskClient: aksk.NewAkSkService(akskService, c),
	}
}

func (s *APIService) GetAkSk(request *restful.Request, response *restful.Response) {
	log.Info("RAJAT - AKSK - GetAkSk ")
	if !policy.Authorize(request, response, "AkSk:get") {
		return
	}

}

func (s *APIService) ListAkSk(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "AkSk:list") {
		return
	}

}

func (s *APIService) CreateAkSk(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "AkSk:create") {
		return
	}

}

func (s *APIService) DeleteAkSk(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "AkSk:delete") {
		return
	}

}
