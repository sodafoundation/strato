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

package s3

import (
	"net/http"
	"encoding/xml"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"
	"golang.org/x/net/context"

	"github.com/opensds/multi-cloud/api/pkg/policy"
	"github.com/opensds/multi-cloud/s3/proto"
	"github.com/opensds/multi-cloud/s3/pkg/model"
)

func (s *APIService) GetStorageClasses(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "storageclass:get") {
		return
	}

	ctx := context.Background()
	//TODO owner
	owner := "test"
	log.Logf("Received request for storage classes.")
	res, err := s.s3Client.GetStorageClasses(ctx, &s3.BaseRequest{Id: owner})
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
		log.Logf("Parse ListStorageClasses error: %v", err)
		response.WriteError(http.StatusInternalServerError, err)
	} else {
		xmlstring = []byte(xml.Header + string(xmlstring))
		response.Write(xmlstring)
		log.Logf("Get List of buckets successfully:%v\n", string(xmlstring))
	}
}

