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
	"github.com/micro/go-log"
	"github.com/opensds/multi-cloud/api/pkg/policy"
	s3 "github.com/opensds/multi-cloud/s3/proto"
	"golang.org/x/net/context"
	"net/http"
)

func (s *APIService) BucketLifecycleDelete(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "bucket:delete") {
		return
	}
	bucketName := request.PathParameter("bucketName")
	ruleID := request.Request.URL.Query()["ruleID"]

	ctx := context.Background()
	log.Logf("Received request for bucket lifecycle delete for bucket: %s and the ruleID: %s", bucketName, ruleID)
	bucket, _ := s.s3Client.GetBucket(ctx, &s3.Bucket{Name: bucketName})
	for _, lcRule := range bucket.LifecycleConfiguration {
		for _, id := range ruleID {
			if lcRule.ID == id {
				deleteInput := s3.DeleteLifecycleInput{Bucket: bucketName, RuleID: id}
				res1, err := s.s3Client.DeleteBucketLifecycle(ctx, &deleteInput)
				if err != nil {
					response.WriteError(http.StatusInternalServerError, err)
					return
				}
				response.WriteEntity(res1)
			}
		}
	}
	log.Log("Delete bucket lifecycle successfully.")
}
