// Copyright 2019 The soda Authors.
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

package s3

import (
	"encoding/xml"
	"net/http"

	"github.com/emicklei/go-restful"
	log "github.com/sirupsen/logrus"

	"github.com/soda/multi-cloud/api/pkg/common"
	"github.com/soda/multi-cloud/api/pkg/filters/signature"
	"github.com/soda/multi-cloud/s3/pkg/model"
	s3 "github.com/soda/multi-cloud/s3/proto"
)

func (s *APIService) GetStorageClasses(request *restful.Request, response *restful.Response) {
	log.Info("Received request for storage classes.")
	err := signature.PayloadCheck(request, response)
	if err != nil {
		WriteErrorResponse(response, request, err)
		return
	}

	ctx := common.InitCtxWithAuthInfo(request)
	res, err := s.s3Client.GetStorageClasses(ctx, &s3.BaseRequest{})
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	tmp := model.ListStorageClasses{}
	classes := []model.StorageClass{}
	for _, v := range res.Classes {
		classes = append(classes, model.StorageClass{Name: v.Name, Tier: v.Tier})
	}
	tmp.Classes = classes

	xmlstring, err := xml.MarshalIndent(tmp, "", "  ")
	if err != nil {
		log.Errorf("parse ListStorageClasses error: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
	} else {
		xmlstring = []byte(xml.Header + string(xmlstring))
		response.Write(xmlstring)
		log.Infof("Get List of storage classes successfully:%v\n", string(xmlstring))
	}
}
