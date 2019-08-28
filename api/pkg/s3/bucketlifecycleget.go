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
	"github.com/opensds/multi-cloud/api/pkg/policy"
	. "github.com/opensds/multi-cloud/api/pkg/utils/constants"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	s3 "github.com/opensds/multi-cloud/s3/proto"
	"golang.org/x/net/context"
)

//Convert function from storage tier to storage class for XML format output
func (s *APIService) tier2class(tier int32) (string, error) {
	{
		mutext.Lock()
		defer mutext.Unlock()
		if len(ClassAndTier) == 0 {
			err := s.loadStorageClassDefinition()
			if err != nil {
				log.Logf("load storage classes failed: %v.\n", err)
				return "", err
			}
		}
	}
	className := ""
	for k, v := range ClassAndTier {
		if v == tier {
			className = k
		}
	}
	if className == "" {
		log.Logf("invalid tier: %d\n", tier)
		return "", fmt.Errorf(InvalidTier)
	}
	return className, nil
}

//Function for GET Bucket Lifecycle API
func (s *APIService) BucketLifecycleGet(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "bucket:get") {
		return
	}
	bucketName := request.PathParameter("bucketName")
	log.Logf("received request for bucket details in GET lifecycle: %s", bucketName)

	ctx := context.Background()
	bucket, _ := s.s3Client.GetBucket(ctx, &s3.Bucket{Name: bucketName})

	// convert back to xml struct
	getLifecycleConf := model.LifecycleConfiguration{}

	// convert lifecycle rule to xml Rule
	if bucket.LifecycleConfiguration != nil {
		for _, lcRule := range bucket.LifecycleConfiguration {
			xmlRule := model.Rule{}

			xmlRule.Status = lcRule.Status
			xmlRule.ID = lcRule.Id
			xmlRule.Filter = converts3FilterToRuleFilter(lcRule.Filter)
			xmlRule.AbortIncompleteMultipartUpload = converts3UploadToRuleUpload(lcRule.AbortIncompleteMultipartUpload)
			xmlRule.Transition = make([]model.Transition, 0)

			//Arranging the transition and expiration actions in XML
			for _, action := range lcRule.Actions {
				log.Logf("action is : %v\n", action)

				if action.Name == ActionNameTransition {
					xmlTransition := model.Transition{}
					xmlTransition.Days = action.Days
					xmlTransition.Backend = action.Backend
					className, err := s.tier2class(action.Tier)
					if err == nil {
						xmlTransition.StorageClass = className
					}
					xmlRule.Transition = append(xmlRule.Transition, xmlTransition)
				}
				if action.Name == ActionNameExpiration {
					xmlExpiration := model.Expiration{}
					xmlExpiration.Days = action.Days
					xmlRule.Expiration = append(xmlRule.Expiration, xmlExpiration)
				}
			}
			// append each xml rule to xml array
			getLifecycleConf.Rule = append(getLifecycleConf.Rule, xmlRule)
		}
	}

	// marshall the array back to xml format
	response.WriteAsXml(getLifecycleConf)
	log.Log("GET lifecycle successful.")
}

func converts3FilterToRuleFilter(filter *s3.LifecycleFilter) model.Filter {
	retFilter := model.Filter{}
	if filter != nil {
		retFilter.Prefix = filter.Prefix
	}
	return retFilter
}

func converts3UploadToRuleUpload(upload *s3.AbortMultipartUpload) model.AbortIncompleteMultipartUpload {
	retUpload := model.AbortIncompleteMultipartUpload{}
	if upload != nil {
		retUpload.DaysAfterInitiation = upload.DaysAfterInitiation
	}
	return retUpload
}
