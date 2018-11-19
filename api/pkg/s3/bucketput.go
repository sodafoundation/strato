// Copyright (c) 2018 Huawei Technologies Co., Ltd. All Rights Reserved.
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
	"github.com/micro/go-log"
	//	"github.com/micro/go-micro/errors"
	"time"

	"github.com/opensds/multi-cloud/api/pkg/policy"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	"github.com/opensds/multi-cloud/s3/proto"
	"golang.org/x/net/context"
)

func (s *APIService) BucketPut(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "bucket:put") {
		return
	}
	bucketName := request.PathParameter("bucketName")

	log.Logf("Received request for create bucket: %s", bucketName)
	ctx := context.Background()
	bucket := s3.Bucket{Name: bucketName}
	body := ReadBody(request)
	//TODO owner
	owner := "test"
	ownerDisplayName := "test"
	bucket.Owner = owner
	bucket.Deleted = false
	bucket.OwnerDisplayName = ownerDisplayName
	bucket.CreationDate = time.Now().Unix()
	//log.Logf("Create bucket body: %s", string(body))
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
				client := getBackendByName(s, backendName)
				if client == nil {
					response.WriteError(http.StatusInternalServerError, NoSuchType.Error())
					return
				}
			} else {
				log.Logf("backetName is %v\n", backendName)
				response.WriteError(http.StatusNotFound, NoSuchBackend.Error())
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
