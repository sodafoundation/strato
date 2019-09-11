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
	"encoding/xml"
	"net/http"
	"time"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"
	"github.com/opensds/multi-cloud/api/pkg/common"
	c "github.com/opensds/multi-cloud/api/pkg/context"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	"github.com/opensds/multi-cloud/s3/proto"
)

func (s *APIService) BucketPut(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	log.Logf("Received request for create bucket: %s", bucketName)

	ctx := common.InitCtxWithAuthInfo(request)
	actx := request.Attribute(c.KContext).(*c.Context)
	bucket := s3.Bucket{Name: bucketName}
	body := ReadBody(request)
	bucket.TenantId = actx.TenantId
	bucket.UserId = actx.UserId
	bucket.Deleted = false
	bucket.CreationDate = time.Now().Unix()

	if body != nil {
		createBucketConf := model.CreateBucketConfiguration{}
		err := xml.Unmarshal(body, &createBucketConf)
		if err != nil {
			response.WriteError(http.StatusInternalServerError, err)
			return
		} else {
			backendName := createBucketConf.LocationConstraint
			if backendName != "" {
				log.Logf("backendName is %v\n", backendName)
				bucket.Backend = backendName
				client := getBackendByName(ctx, s, backendName)
				if client == nil {
					response.WriteError(http.StatusInternalServerError, NoSuchType.Error())
					return
				}
			} else {
				log.Log("default backend is not provided.")
				response.WriteError(http.StatusBadRequest, NoSuchBackend.Error())
				return
			}
		}
	}

	res, err := s.s3Client.CreateBucket(ctx, &bucket)
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	log.Log("Create bucket successfully.")
	response.WriteEntity(res)
}
