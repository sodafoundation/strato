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
	"crypto/md5"
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/emicklei/go-restful"
	"github.com/opensds/multi-cloud/api/pkg/common"
	. "github.com/opensds/multi-cloud/api/pkg/utils/constants"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	"github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

// Map from storage calss to tier
var ClassAndTier1 map[string]int32
var mutext1 sync.Mutex

func (s *APIService) BucketSSEPut(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	log.Infof("received request for PUT bucket SSE: %s", bucketName)

	ctx := common.InitCtxWithAuthInfo(request)
	bucket, err := s.s3Client.GetBucket(ctx, &s3.Bucket{Name: bucketName})
	if err != nil {
		log.Errorf("get bucket failed, err=%v\n", err)
		response.WriteError(http.StatusInternalServerError, fmt.Errorf("bucket does not exist"))
	}

	body := ReadBody(request)
	log.Infof("MD5 sum for request body is %x", md5.Sum(body))

	if body != nil {
		sseConf := model.SSEConfiguration{}
		errSSE := xml.Unmarshal(body, &sseConf)
		if errSSE != nil {
			response.WriteError(http.StatusInternalServerError, err)
			return
		}

		s3SSE  := &s3.ServerSideEncryption{
			SseType:              "",
			EncryptionKey:        nil,
			InitilizationVector:  nil,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		}
		if sseConf.SSE.Enabled=="true"{
			s3SSE.SseType = "SSE"
		}

		bucket.BucketMeta.ServerSideEncryption = s3SSE

		s.s3Client.UpdateBucket(ctx, bucket.BucketMeta)

		createLifecycleConf := model.LifecycleConfiguration{}
		err := xml.Unmarshal(body, &createLifecycleConf)
		if err != nil {
			response.WriteError(http.StatusInternalServerError, err)
			return
		} else {
			dupIdCheck := make(map[string]interface{})
			s3RulePtrArr := make([]*s3.LifecycleRule, 0)
			for _, rule := range createLifecycleConf.Rule {
				s3Rule := s3.LifecycleRule{}

				//check if the ruleID has any duplicate values
				if _, ok := dupIdCheck[rule.ID]; ok {
					log.Errorf("duplicate ruleID found for rule : %s\n", rule.ID)
					ErrStr := strings.Replace(DuplicateRuleIDError, "$1", rule.ID, 1)
					response.WriteError(http.StatusBadRequest, fmt.Errorf(ErrStr))
					return
				}
				// Assigning the rule ID
				dupIdCheck[rule.ID] = struct{}{}
				s3Rule.Id = rule.ID

				//Assigning the status value to s3 status
				log.Infof("status in rule file is %v\n", rule.Status)
				s3Rule.Status = rule.Status

				//Assigning the filter, using convert function to convert xml struct to s3 struct
				s3Rule.Filter = convertRuleFilterToS3Filter(rule.Filter)

				// Create the type of transition array
				s3ActionArr := make([]*s3.Action, 0)

				for _, transition := range rule.Transition {

					//Defining the Transition array and assigning the values tp populate fields
					s3Transition := s3.Action{Name: ActionNameTransition}

					//Assigning the value of days for transition to happen
					s3Transition.Days = transition.Days

					//Assigning the backend value to the s3 struct
					s3Transition.Backend = transition.Backend

					//Assigning the storage class of the object to s3 struct
					tier, err := s.class2tier(transition.StorageClass)
					if err != nil {
						response.WriteError(http.StatusBadRequest, err)
						return
					}
					s3Transition.Tier = tier

					//Adding the transition value to the main rule
					s3ActionArr = append(s3ActionArr, &s3Transition)
				}

				//Loop for getting the values from xml struct
				for _, expiration := range rule.Expiration {
					s3Expiration := s3.Action{Name: ActionNameExpiration}
					s3Expiration.Days = expiration.Days
					s3ActionArr = append(s3ActionArr, &s3Expiration)
				}

				//validate actions
				err := checkValidationOfActions(s3ActionArr)
				if err != nil {
					log.Errorf("validation of actions failed: %v\n", err)
					response.WriteError(http.StatusBadRequest, err)
					return
				}

				//Assigning the Expiration action to s3 struct expiration
				s3Rule.Actions = s3ActionArr

				s3Rule.AbortIncompleteMultipartUpload = convertRuleUploadToS3Upload(rule.AbortIncompleteMultipartUpload)

				// add to the s3 array
				s3RulePtrArr = append(s3RulePtrArr, &s3Rule)
			}
			// assign lifecycle rules to s3 bucket
			bucket.BucketMeta.LifecycleConfiguration = s3RulePtrArr
		}
	} else {
		log.Info("no request body provided for creating lifecycle configuration")
		response.WriteError(http.StatusBadRequest, fmt.Errorf(NoRequestBodyLifecycle))
		return
	}

	// Create bucket with bucket name will check if the bucket exists or not, if it exists
	// it will internally call UpdateBucket function
	res, err := s.s3Client.UpdateBucket(ctx, bucket.BucketMeta)
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	log.Info("create bucket lifecycle successful.")
	response.WriteEntity(res)

}
