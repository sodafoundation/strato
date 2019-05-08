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
	"github.com/opensds/multi-cloud/s3/pkg/model"
	s3 "github.com/opensds/multi-cloud/s3/proto"
	"golang.org/x/net/context"
)

func (s *APIService) BucketLifecycleGet(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "bucket:get") {
		return
	}
	bucketName := request.PathParameter("bucketName")
	log.Logf("Received request for bucket details in GET lifecycle: %s", bucketName)

	ctx := context.Background()
	bucket, _ := s.s3Client.GetBucket(ctx, &s3.Bucket{Name: bucketName})

	// convert back to xml struct
	getLifecycleConf := model.LifecycleConfiguration{}
	getLifecycleConf.Rule = make([]model.Rule, 0)
	// convert lifecycle rule to xml Rule
	for _, lcRule := range bucket.LifecycleConfiguration {
		xmlRule := model.Rule{}

		xmlRule.Status = lcRule.Status
		xmlRule.ID = lcRule.ID
		xmlRule.Filter = converts3FilterToRuleFilter(lcRule.Filter)
		xmlRule.AbortIncompleteMultipartUpload = converts3UploadToRuleUpload(lcRule.AbortIncompleteMultipartUpload)
		xmlRule.Transition = make([]model.Transition, 0)
		for _, transition := range lcRule.Transition {
			xmlTransition := model.Transition{}
			xmlTransition.Days = transition.Days
			xmlTransition.StorageClass = transition.StorageClass
			xmlTransition.Backend = transition.Backend
			xmlRule.Transition = append(xmlRule.Transition, xmlTransition)
		}
		for _, expiration := range lcRule.Expiration {
			xmlExpiration := model.Expiration{}
			xmlExpiration.Days = expiration.Days
			xmlRule.Expiration = append(xmlRule.Expiration, xmlExpiration)
		}
		// append each xml rule to xml array
		getLifecycleConf.Rule = append(getLifecycleConf.Rule, xmlRule)
	}

	// marshall the array back to xml
	//xmlByteArr, _ := xml.Marshal(getLifecycleConf.Rule)
	response.WriteAsXml(getLifecycleConf.Rule)

	log.Log("Get bucket lifecycle successfully.")
}

func converts3FilterToRuleFilter(filter *s3.LifecycleFilter) model.Filter {
	retFilter := model.Filter{}
	retFilter.Prefix = filter.Prefix
	return retFilter
}

func converts3UploadToRuleUpload(upload *s3.AbortMultipartUpload) model.AbortIncompleteMultipartUpload {
	retUpload := model.AbortIncompleteMultipartUpload{}
	retUpload.DaysAfterInitiation = upload.DaysAfterInitiation
	return retUpload
}
