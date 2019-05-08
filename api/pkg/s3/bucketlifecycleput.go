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
	"encoding/xml"
	"net/http"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"

	"github.com/opensds/multi-cloud/api/pkg/policy"
	//. "github.com/opensds/multi-cloud/s3/pkg/exception"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	"github.com/opensds/multi-cloud/s3/proto"
	"golang.org/x/net/context"
	"crypto/md5"
)

func (s *APIService) BucketLifecyclePut(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "bucket:put") {
		return
	}
	bucketName := request.PathParameter("bucketName")
	log.Logf("Received request for create bucket: %s", bucketName)
	ctx := context.Background()
	bucket, _ := s.s3Client.GetBucket(ctx, &s3.Bucket{Name: bucketName})
	body := ReadBody(request)
	log.Logf("Body request is %v\n", body)
	log.Logf("%x", md5.Sum(body))

	if body != nil {
		createLifecycleConf := model.LifecycleConfiguration{}
		log.Logf("Before unmarshal struct is %v\n", createLifecycleConf)
		err := xml.Unmarshal(body, &createLifecycleConf)
		log.Logf("After unmarshal struct is %v\n", createLifecycleConf)
		if err != nil {
			response.WriteError(http.StatusInternalServerError, err)
			return
		} else {
			s3RulePtrArr := make([]*s3.LifecycleRule,0)
			for _, rule := range createLifecycleConf.Rule{
				s3Rule := s3.LifecycleRule{}

				// assign the fields
				s3Rule.ID = rule.ID  //Assigning the rule ID

				//Assigning the status value to s3 status
				log.Logf("Status in rule file is %v\n", rule.Status)
				s3Rule.Status = rule.Status

				//Assigning the filter, using convert function to convert xml struct to s3 struct
				s3Rule.Filter = convertRuleFilterToS3Filter(rule.Filter)

				// Create the type of transition array
				s3TransitionArr := make([]*s3.TransitionAction,0)

				for _, transition := range rule.Transition{

					//Defining the Transition array and assigning the values tp populate fields
					s3Transition := s3.TransitionAction{}

					//Assigning the value of days for transition to happen
					s3Transition.Days = transition.Days

					//Assigning the backend value to the s3 struct
					s3Transition.Backend = transition.Backend

					//Assigning the storage class of the object to s3 struct
					s3Transition.StorageClass = transition.StorageClass

					//Adding the transition value to the main rule
					s3TransitionArr = append(s3TransitionArr, &s3Transition)
				}

				//Assigning the transition action in the s3 rule
				s3Rule.Transition = s3TransitionArr

				//Create the type of Expiration Array
				s3ExpirationArr := make([]*s3.ExpirationAction,0)

				//Loop for getting the values from xml struct
				for _, expiration := range rule.Expiration {
					s3Expiration := s3.ExpirationAction{}
					s3Expiration.Days = expiration.Days
					s3ExpirationArr = append(s3ExpirationArr, &s3Expiration)
				}
				//Assigning the Expiration action to s3 struct expiration
				s3Rule.Expiration = s3ExpirationArr

				s3Rule.AbortIncompleteMultipartUpload = convertRuleUploadToS3Upload(rule.AbortIncompleteMultipartUpload)
				// add to the s3 array
				s3RulePtrArr = append(s3RulePtrArr, &s3Rule)
			}
			// assign lifecycle rules to s3 bucket
			bucket.LifecycleConfiguration = s3RulePtrArr
			log.Logf("final bucket metadata is %v\n", bucket)
		}
	}

	// Create bucket with bucket name will check if the bucket exists or not, if it exists
	// it will internally call UpdateBucket function
	res, err := s.s3Client.CreateBucket(ctx, bucket)
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	log.Log("Create bucket lifecycle successfully.")
	response.WriteEntity(res)

}

func convertRuleFilterToS3Filter(filter model.Filter) *s3.LifecycleFilter{
	retFilter := s3.LifecycleFilter{}
	retFilter.Prefix = filter.Prefix
	return &retFilter
}

func convertRuleUploadToS3Upload(upload model.AbortIncompleteMultipartUpload) *s3.AbortMultipartUpload{
	retUpload := s3.AbortMultipartUpload{}
	retUpload.DaysAfterInitiation = upload.DaysAfterInitiation
	return &retUpload
}


