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
	"reflect"

	//"sort"
	"strings"
	"sync"

	. "github.com/opensds/multi-cloud/api/pkg/utils/constants"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"

	"github.com/opensds/multi-cloud/api/pkg/policy"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	s3 "github.com/opensds/multi-cloud/s3/proto"
	"golang.org/x/net/context"
)

// Map from storage calss to tier
var ClassAndTier map[string]int32
var mutext sync.Mutex

func (s *APIService) loadStorageClassDefinition() error {
	ctx := context.Background()
	log.Log("Load storage classes.")
	res, err := s.s3Client.GetStorageClasses(ctx, &s3.BaseRequest{})
	if err != nil {
		log.Logf("get storage classes from s3 service failed: %v\n", err)
		return err
	}
	ClassAndTier = make(map[string]int32)
	for _, v := range res.Classes {
		ClassAndTier[v.Name] = v.Tier
	}
	return nil
}

func (s *APIService) class2tier(name string) (int32, error) {
	{
		mutext.Lock()
		defer mutext.Unlock()
		if len(ClassAndTier) == 0 {
			err := s.loadStorageClassDefinition()
			if err != nil {
				log.Logf("load storage classes failed: %v.\n", err)
				return 0, err
			}
		}
	}
	tier, ok := ClassAndTier[name]
	if !ok {
		log.Logf("translate storage class name[%s] to tier failed: %s.\n", name)
		return 0, fmt.Errorf("invalid storage class:%s", name)
	}
	log.Logf("class[%s] to tier[%d]\n", name, tier)
	return tier, nil
}

func checkValidationOfActions(actions []*s3.Action) error {
	var pre *s3.Action = nil
	for _, action := range actions {
		log.Logf("action: %+v\n", *action)
		if pre == nil {
			if action.Name == ActionNameExpiration && action.Days < ExpirationMinDays {
				// If only an expiration action for a rule, the days for that action should be more than ExpirationMinDays
				return fmt.Errorf("error: Days for Expiring object must not be less than %d", ExpirationMinDays)
			}
			if action.Name == ActionNameTransition && action.Days < TransitionMinDays {
				// the days for transition to tiers except tier999 should not less than TransitionMinDays
				minDays := int32(TransitionMinDays)
				if action.Tier == Tier999 {
					// the days for transition to tier999 should not less than TransitionToArchiveMinDays
					minDays = TransitionToArchiveMinDays
				}
				if action.Days < minDays {
					return fmt.Errorf("error: days for transitioning object to tier_%d must not be less than %d",
						action.Tier, minDays)
				}

			}
		} else {
			if pre.Name == ActionNameExpiration {
				// Only one expiration action for each rule is supported
				return fmt.Errorf(MoreThanOneExpirationAction)
			}

			if action.Name == ActionNameExpiration && pre.Days+ExpirationMinDays > action.Days {
				return fmt.Errorf(DaysInStorageClassBeforeExpiration)
			}

			if action.Name == ActionNameTransition && pre.Days+LifecycleTransitionDaysStep > action.Days {
				return fmt.Errorf(DaysInStorageClassBeforeTransition)
			}
		}
		pre = action
	}
	return nil
}

func (s *APIService) checkModifyLifecycleConfig(bucketLcCfg *s3.LifecycleRule, rule *s3.LifecycleRule) bool {
	//check if the rule already exists in lifecycle configuration of the bucket
	var modifyFlg = false

	res := reflect.DeepEqual(bucketLcCfg, rule)
	if !res {
		modifyFlg = true
		log.Logf("update lifecycle rule is requested")
	}
	return modifyFlg
}

func (s *APIService) BucketLifecyclePut(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "bucket:put") {
		return
	}
	bucketName := request.PathParameter("bucketName")
	log.Logf("received request for create bucket lifecycle: %s", bucketName)
	ctx := context.Background()
	bucket, _ := s.s3Client.GetBucket(ctx, &s3.Bucket{Name: bucketName})
	body := ReadBody(request)
	log.Logf("MD5 sum for body is %x", md5.Sum(body))
	modifyFlg := false
	newbucket := &s3.Bucket{}
	finalbucket := bucket

	if body != nil {
		createLifecycleConf := model.LifecycleConfiguration{}
		err := xml.Unmarshal(body, &createLifecycleConf)
		if err != nil {
			response.WriteError(http.StatusInternalServerError, err)
			return
		} else {
			dupIdCheck := make(map[string]interface{})
			for _, rule := range createLifecycleConf.Rule {
				s3Rule := s3.LifecycleRule{}

				//check if the ruleID has any duplicate values
				if _, ok := dupIdCheck[rule.ID]; ok {
					log.Logf("duplicate ruleID found for rule : %s\n", rule.ID)
					ErrStr := strings.Replace(DuplicateRuleIDError, "$1", rule.ID, 1)
					response.WriteError(http.StatusBadRequest, fmt.Errorf(ErrStr))
					return
				}
				// Assigning the rule ID
				dupIdCheck[rule.ID] = struct{}{}
				s3Rule.Id = rule.ID

				//Assigning the status value to s3 status
				log.Logf("status in rule file is %v\n", rule.Status)
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
					log.Logf("validation of actions failed: %v\n", err)
					response.WriteError(http.StatusBadRequest, err)
					return
				}

				//Assigning the Expiration action to s3 struct expiration
				s3Rule.Actions = s3ActionArr

				s3Rule.AbortIncompleteMultipartUpload = convertRuleUploadToS3Upload(rule.AbortIncompleteMultipartUpload)

				//check if the there is any modification in rule if it already exists
				//TODO need to write another modify lifecycle API
				//Fixme
				lcCfg := bucket.GetLifecycleConfiguration()
				for _, cfg := range lcCfg {
					if cfg.Id == s3Rule.Id {
						isModifyRequest := s.checkModifyLifecycleConfig(cfg, &s3Rule)
						if isModifyRequest {

							/*
								set the modify flag
							 */
							modifyFlg = true

							/*
							Delete previous existing rule from bucket and add new rule with changes
							 */
							deleteLcInput := s3.DeleteLifecycleInput{Bucket: bucket.Name, RuleID: cfg.Id}
							_, err := s.s3Client.DeleteBucketLifecycle(ctx, &deleteLcInput)
							if err != nil {
								response.WriteError(http.StatusBadRequest, err)
								return
							}

							/*
							get updated bucket from database after deleting the old rule
							 */
							newbucket, _ = s.s3Client.GetBucket(ctx, &s3.Bucket{Name: bucketName})
						} else {

							/*
							no changes in rule and it is been called again to create
							 */
							log.Logf("this rule already exists in configuration : %s\n", rule.ID)
							ErrStr := strings.Replace(RuleIDAlreadyExistError, "$1", rule.ID, 1)
							response.WriteError(http.StatusBadRequest, fmt.Errorf(ErrStr))
							return
						}
					}
				}
				if modifyFlg{
					finalbucket = newbucket
				}
				finalbucket.LifecycleConfiguration = append(finalbucket.LifecycleConfiguration, &s3Rule)
			}
		}
	} else {
		log.Log("no request body provided for creating lifecycle configuration")
		response.WriteError(http.StatusBadRequest, fmt.Errorf(NoRequestBodyLifecycle))
		return
	}

	// Create bucket with bucket name will check if the bucket exists or not, if it exists
	// it will internally call UpdateBucket function
	res, err := s.s3Client.UpdateBucket(ctx, finalbucket)
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	log.Log("create bucket lifecycle successful.")
	response.WriteEntity(res)

}

func convertRuleFilterToS3Filter(filter model.Filter) *s3.LifecycleFilter {
	retFilter := s3.LifecycleFilter{}
	/*
		check if prefix is not empty
	*/
	if filter.Prefix != "" {
		retFilter.Prefix = filter.Prefix
		return &retFilter
	} else {
		return nil
	}
}

func convertRuleUploadToS3Upload(upload model.AbortIncompleteMultipartUpload) *s3.AbortMultipartUpload {
	retUpload := s3.AbortMultipartUpload{}
	retUpload.DaysAfterInitiation = upload.DaysAfterInitiation
	return &retUpload
}
