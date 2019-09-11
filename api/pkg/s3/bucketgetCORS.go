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
	"fmt"
	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"
	"github.com/opensds/multi-cloud/api/pkg/common"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	s3 "github.com/opensds/multi-cloud/s3/proto"
	"net/http"
)

func (s *APIService) BucketgetCORS(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	log.Logf("Received request for CORS bucket: %s", bucketName)

	ctx := common.InitCtxWithAuthInfo(request)
	bucket, err := s.s3Client.GetBucket(ctx, &s3.Bucket{Name: bucketName})
	if err != nil {
		log.Logf("get bucket failed, err=%v\n", err)
		response.WriteError(http.StatusInternalServerError, fmt.Errorf("bucket does not exist"))
	}

	// convert back to xml struct
	getCorsConf := model.Cors{}

	// convert CORS rule to xml Rule
	if bucket.CorsConfiguration != nil {
		for _, lcRule := range bucket.CorsConfiguration {
			xmlRule := model.CorsRule{}

			// Convert Allowed methods from json to xml struct
			xmlRule.AllowedMethods = lcRule.AllowedMethods

			// Convert CORS config Id from json to xml struct
			xmlRule.Id = lcRule.Id

			// Convert Allowed origins from json to xml struct
			xmlRule.AllowedOrigins = lcRule.AllowedOrigins

			// Convert Allowed headers from json to xml struct
			xmlRule.AllowedHeaders = lcRule.AllowedHeaders

			// Convert Exposed headers from json to xml struct
			xmlRule.ExposedHeaders = lcRule.ExposedHeaders

			// append each xml rule to xml array
			getCorsConf.CorsRules = append(getCorsConf.CorsRules, xmlRule)
		}
	}

	// marshall the array back to xml format and write it
	response.WriteAsXml(getCorsConf)
	log.Log("GET CORS successful.")
}

