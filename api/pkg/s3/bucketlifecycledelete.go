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
	"strings"

	"github.com/emicklei/go-restful"
	"github.com/opensds/multi-cloud/api/pkg/common"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	"github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

func (s *APIService) BucketLifecycleDelete(request *restful.Request, response *restful.Response) {
	ctx := common.InitCtxWithAuthInfo(request)
	bucketName := request.PathParameter("bucketName")
	ruleID := request.Request.URL.Query()["ruleID"]
	log.Infof("received request for deleting lifecycle rule[id=%s] for bucket[name=%s].\n", ruleID, bucketName)

	if ruleID == nil {
		response.WriteErrorString(http.StatusBadRequest, NoRuleIDForLifecycleDelete)
		return
	}

	//var foundID int
	FoundIDArray := []string{}
	NonFoundIDArray := []string{}

	bucket, err := s.s3Client.GetBucket(ctx, &s3.Bucket{Name: bucketName})
	if err != nil {
		log.Infof("get bucket[name=%s] failed, err=%v.\n", bucketName, err)
		response.WriteError(http.StatusInternalServerError, NoSuchBucket.Error())
		return
	}

	for _, id := range ruleID {
		isfound := false
		for _, lcRule := range bucket.LifecycleConfiguration {
			if lcRule.Id == id {
				isfound = true
				FoundIDArray = append(FoundIDArray, id)
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
		deleteInput := s3.DeleteLifecycleInput{Bucket: bucketName, RuleID: id}
		res, err := s.s3Client.DeleteBucketLifecycle(ctx, &deleteInput)
		if err != nil {
			log.Infof("delete lifecycle[id=%s] of bucket[name=%s] failed, err=%v.\n", id, bucketName, err)
			response.WriteError(http.StatusBadRequest, err)
			return
		}
		response.WriteEntity(res)
	}
	log.Info("delete bucket lifecycle successful.")
}
