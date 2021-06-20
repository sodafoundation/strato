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
	aksk "github.com/opensds/multi-cloud/aksk/proto"
	"github.com/opensds/multi-cloud/api/pkg/common"
	c "github.com/opensds/multi-cloud/api/pkg/context"
	"github.com/opensds/multi-cloud/api/pkg/policy"
	log "github.com/sirupsen/logrus"
	"net/http"
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

	log.Infof("Received request for AK, SK details - : %s\n", request.PathParameter("id"))
	id := request.PathParameter("id")

	akskDetail := &aksk.AkSkDetail{}
	ctx := common.InitCtxWithAuthInfo(request)
	actx := request.Attribute(c.KContext).(*c.Context)
	akskDetail.ProjectId = actx.TenantId
	akskDetail.UserId = actx.UserId
	akskDetail.Token = actx.AuthToken

	res, err := s.akskClient.GetAkSk(ctx, &aksk.GetAkSkRequest{Id: id, AkSkDetail: akskDetail})
	if err != nil {
		log.Errorf("failed to get AK, SK details: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Info("Get AK, SK details completed successfully.")
	response.WriteEntity(res.AkSkDetail)

}

/*func (s *APIService) ListAkSks(request *restful.Request, response *restful.Response) {
	log.Info("RAJAT - AKSK - ListAkSk ")
	if !policy.Authorize(request, response, "AkSk:list") {
		return
	}

	akskDetail := &aksk.AkSkDetail{}
	ctx := common.InitCtxWithAuthInfo(request)
	actx := request.Attribute(c.KContext).(*c.Context)
	akskDetail.ProjectId = actx.TenantId
	akskDetail.UserId = actx.UserId
	akskDetail.Token = actx.AuthToken

	res, err := s.akskClient.ListAkSk(ctx, &aksk.ListAkSkRequest{})
	if err != nil {
		log.Errorf("failed to get AK, SK details: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Info("Get AK, SK details completed successfully.")
	response.WriteEntity(res.AkSkDetail)

}*/

func (s *APIService) CreateAkSk(request *restful.Request, response *restful.Response) {
	log.Info("RAJAT - AKSK - CreateAkSk ")
	if !policy.Authorize(request, response, "AkSk:create") {
		return
	}

	akskDetail := &aksk.AkSkDetail{}
	err := request.ReadEntity(&akskDetail)
	if err != nil {
		log.Errorf("failed to read request body: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	ctx := common.InitCtxWithAuthInfo(request)
	actx := request.Attribute(c.KContext).(*c.Context)
	akskDetail.ProjectId = actx.TenantId
	akskDetail.UserId = actx.UserId
	akskDetail.Token = actx.AuthToken

	log.Info("RAJAT - AKSK - ProjectID ", akskDetail.ProjectId )
	log.Info("RAJAT - AKSK - UserID ", akskDetail.UserId )
	log.Info("RAJAT - AKSK - Token ", akskDetail.Token )

	res, err := s.akskClient.CreateAkSk(ctx, &aksk.CreateAkSkRequest{Aksk: akskDetail})
	if err != nil {
		log.Errorf("failed to create Ak, SK: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Info("Create AcessKey, SecretKey successfully.")
	response.WriteEntity(res.Aksk)

}

func (s *APIService) DeleteAkSk(request *restful.Request, response *restful.Response) {
	log.Info("RAJAT - AKSK - DeleteAkSk ")
	if !policy.Authorize(request, response, "AkSk:delete") {
		return
	}

	log.Infof("Received DELETE request for AK, SK details - : %s\n", request.PathParameter("id"))
	id := request.PathParameter("id")

	akskDetail := &aksk.AkSkDetail{}
	ctx := common.InitCtxWithAuthInfo(request)
	actx := request.Attribute(c.KContext).(*c.Context)
	akskDetail.ProjectId = actx.TenantId
	akskDetail.UserId = actx.UserId
	akskDetail.Token = actx.AuthToken

	res, err := s.akskClient.DeleteAkSk(ctx, &aksk.DeleteAkSkRequest{Id: id, AkSkDetail: akskDetail})
	if err != nil {
		log.Errorf("failed to get AK, SK details: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Info("Get AK, SK details completed successfully.")
	response.WriteEntity(res)
}
