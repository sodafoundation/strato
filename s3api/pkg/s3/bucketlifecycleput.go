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
	"sync"

	"github.com/emicklei/go-restful"
	. "github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	"github.com/opensds/multi-cloud/s3/pkg/utils"
	"github.com/opensds/multi-cloud/s3/proto"
	"github.com/opensds/multi-cloud/s3api/pkg/common"
	. "github.com/opensds/multi-cloud/s3api/pkg/utils/constants"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

// Map from storage calss to tier
var ClassAndTier map[string]int32
var mutext sync.Mutex

func (s *APIService) loadStorageClassDefinition() error {
	ctx := context.Background()
	log.Info("Load storage classes.")
	res, err := s.s3Client.GetStorageClasses(ctx, &s3.BaseRequest{})
	if err != nil {
		log.Errorf("get storage classes from s3 service failed: %v\n", err)
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
				log.Errorf("load storage classes failed: %v.\n", err)
				return 0, err
			}
		}
	}
	tier, ok := ClassAndTier[name]
	if !ok {
		log.Errorf("translate storage class name[%s] to tier failed: %s.\n", name)
		return 0, fmt.Errorf("invalid storage class:%s", name)
	}
	log.Infof("class[%s] to tier[%d]\n", name, tier)
	return tier, nil
}

func checkValidationOfActions(actions []*s3.Action) error {
	var pre *s3.Action = nil
	for _, action := range actions {
		log.Infof("action: %+v\n", *action)
		if pre == nil {
			if action.Name == ActionNameExpiration && action.Days < ExpirationMinDays {
				// If only an expiration action for a rule, the days for that action should be more than ExpirationMinDays
				return fmt.Errorf(InvalidExpireDays, ExpirationMinDays)
			}
			if action.Name == ActionNameTransition && action.Days < TransitionMinDays {
				// the days for transition to tiers except tier999 should not less than TransitionMinDays
				minDays := int32(TransitionMinDays)
				if action.Tier == utils.Tier999 {
					// the days for transition to tier999 should not less than TransitionToArchiveMinDays
					minDays = TransitionToArchiveMinDays
				}
				if action.Days < minDays {
					return fmt.Errorf(InvalidTransistionDays, action.Tier, minDays)
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

func validLcStatus(status string) bool {
	switch status {
	case "Enabled":
	case "Disabled":
		return true
	default:
		log.Errorln("invalid lc status:", status)
	}

	return false
}

func (s *APIService) BucketLifecyclePut(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	log.Infof("received request for creating lifecycle of bucket: %s", bucketName)

	ctx := common.InitCtxWithAuthInfo(request)
	_, err := s.getBucketMeta(ctx, bucketName)
	if err != nil {
		log.Errorf("get bucket[%s] failed, err=%v\n", bucketName, err)
		WriteErrorResponse(response, request, err)
		return
	}

	body := ReadBody(request)
	log.Infof("MD5 sum for body is %x", md5.Sum(body))
	if body == nil {
		log.Info("no request body provided for creating lifecycle configuration")
		WriteErrorResponse(response, request, S3ErrorCode(ErrInvalidLc))
		return
	}

	createLifecycleConf := model.LifecycleConfiguration{}
	err = xml.Unmarshal(body, &createLifecycleConf)
	if err != nil {
		log.Errorf("unmarshal error:%v\n", err)
		WriteErrorResponse(response, request, S3ErrorCode(ErrInvalidLc))
		return
	}

	dupIdCheck := make(map[string]interface{})
	s3RulePtrArr := make([]*s3.LifecycleRule, 0)
	ruleCount := 0
	for _, rule := range createLifecycleConf.Rule {
		if ruleCount > 1000 {
			log.Error("too many rules\n")
			WriteApiErrorResponse(response, request, http.StatusBadRequest, AWSErrCodeInvalidArgument,
				fmt.Sprintf(TooMuchLCRuls, 1000))
			return
		}

		s3Rule := s3.LifecycleRule{}

		//check if the ruleID has any duplicate values
		if _, ok := dupIdCheck[rule.ID]; ok {
			log.Errorf("duplicate ruleID found for rule : %s\n", rule.ID)
			WriteApiErrorResponse(response, request, http.StatusBadRequest, AWSErrCodeInvalidArgument,
				fmt.Sprintf(DuplicateRuleIDError, rule.ID))
			return
		}
		// Assigning the rule ID
		dupIdCheck[rule.ID] = struct{}{}
		s3Rule.Id = rule.ID

		//Assigning the status value to s3 status
		log.Infof("status in rule file is %v\n", rule.Status)
		s3Rule.Status = rule.Status
		if !validLcStatus(s3Rule.Status) {
			WriteErrorResponse(response, request, ErrMalformedXML)
			return
		}

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
			ruleCount++
		}

		//Loop for getting the values from xml struct
		for _, expiration := range rule.Expiration {
			s3Expiration := s3.Action{Name: ActionNameExpiration}
			s3Expiration.Days = expiration.Days
			s3ActionArr = append(s3ActionArr, &s3Expiration)
			ruleCount++
		}

		//validate actions
		err := checkValidationOfActions(s3ActionArr)
		if err != nil {
			log.Errorf("validation of actions failed: %v\n", err)
			WriteApiErrorResponse(response, request, http.StatusBadRequest, AWSErrCodeInvalidArgument, err.Error())
			return
		}

		//Assigning the Expiration action to s3 struct expiration
		s3Rule.Actions = s3ActionArr

		s3Rule.AbortIncompleteMultipartUpload = convertRuleUploadToS3Upload(rule.AbortIncompleteMultipartUpload)

		// add to the s3 array
		s3RulePtrArr = append(s3RulePtrArr, &s3Rule)
	}

	lcRsp, err := s.s3Client.PutBucketLifecycle(ctx, &s3.PutBucketLifecycleRequest{BucketName: bucketName, Lc: s3RulePtrArr})
	if HandleS3Error(response, request, err, lcRsp.GetErrorCode()) != nil {
		log.Errorf("put bucket[%s] lifecycle failed, err=%v, errCode=%d\n", bucketName, err, lcRsp.GetErrorCode())
		return
	}

	log.Info("create bucket lifecycle successful.")
	WriteSuccessResponse(response, nil)
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
