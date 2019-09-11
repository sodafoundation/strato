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
	"github.com/emicklei/go-restful"
	"net/http"
	"strings"

	"github.com/opensds/multi-cloud/api/pkg/common"
	s3 "github.com/opensds/multi-cloud/s3/proto"
)

func (s *APIService) BucketDeleteCORS(request *restful.Request, response *restful.Response) {

	ctx := common.InitCtxWithAuthInfo(request)
	FoundIDArray := []string{}
	NonFoundIDArray := []string{}
	bucketName := request.PathParameter("bucketName")
	corsID := request.Request.URL.Query()["corsID"]
	if corsID != nil {
		bucket, _ := s.s3Client.GetBucket(ctx, &s3.Bucket{Name: bucketName})
		for _, id := range corsID {
			isfound := false
			for _, lcRule := range bucket.CorsConfiguration {
				if lcRule.Id == id {
					isfound = true
					FoundIDArray = append(FoundIDArray, id)
					break
				}
			}
			if !isfound {
				NonFoundIDArray = append(NonFoundIDArray, id)
			}
		}
		for _, id := range NonFoundIDArray {
			response.WriteErrorString(http.StatusBadRequest, strings.Replace("error: rule ID $1 doesn't exist \n\n", "$1", id, 1))
		}

		for _, id := range FoundIDArray {
			deleteInput := s3.DeleteCORSInput{Bucket: bucketName, CorsID: id}
			res, err := s.s3Client.DeleteBucketCORS(ctx, &deleteInput)
			if err != nil {
				response.WriteError(http.StatusBadRequest, err)
				return
			}
			response.WriteEntity(res)
		}

	} else {
		response.WriteErrorString(http.StatusBadRequest, NoRuleIDForCORSDelete)
		return
	}
	log.Log("delete bucket CORS configuration successful.")
}
