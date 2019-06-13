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
	"fmt"
	"net/http"
	"strings"

	"github.com/opensds/multi-cloud/s3/pkg/model"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	s3 "github.com/opensds/multi-cloud/s3/proto"
	"golang.org/x/net/context"

	"github.com/opensds/multi-cloud/api/pkg/policy"
	. "github.com/opensds/multi-cloud/api/pkg/utils/constants"
)

func (s *APIService) ObjectRestore(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "object:restore") {
		return
	}
	url := request.Request.URL
	log.Logf("URL is %v", request.Request.URL.String())

	/*
		parsing the parameters from the URL request
	*/
	bucketName := request.PathParameter("bucketName")
	objectKey := request.PathParameter("objectKey")

	if strings.HasSuffix(url.String(), "/") {
		objectKey = objectKey + "/"
	}

	/*
		parsing the request body from the query
	*/
	body := ReadBody(request)
	if body != nil {

		/*
			unmarshal restore object xml struct into s3 struct
		*/
		createRestoreConf := model.RestoreRequest{}
		err := xml.Unmarshal(body, &createRestoreConf)
		if err != nil {
			response.WriteError(http.StatusInternalServerError, err)
			return
		}
		ctx := context.Background()
		/*
			validate if specified bucket exists or not
		*/
		BucketMD, _ := s.s3Client.GetBucket(ctx, &s3.Bucket{Name: bucketName})
		if BucketMD == nil {
			response.WriteError(http.StatusInternalServerError, fmt.Errorf("error: specified bucket does not exist"))
			return
		}
		/*
			creating Get object input
		*/
		objectInput := s3.GetObjectInput{Bucket: bucketName, Key: objectKey}

		/*
			getting the object metadata from database using GET object input
		*/
		objectMD, _ := s.s3Client.GetObject(ctx, &objectInput)

		if objectMD != nil {
			client := getBackendByName(s, objectMD.Backend)
			/*
				creating the restore object input
			*/
			restoreInput := s3.RestoreObjectInput{Key: objectKey, Bucket: bucketName, Days: createRestoreConf.Days, Tier: createRestoreConf.GlacierJobParameters.Tier}

			/*
				call for restoring the object
			*/
			s3restore := s3.ObjectRestoreStatus{}
			s3restore.RestoreMarker = true

			s3err := client.RestoreObject(&restoreInput, ctx)
			if s3err.Code != ERR_OK {
				s3restore.RestoreState = ActionStateRestoring
				response.WriteError(http.StatusInternalServerError, s3err.Error())
				return
			} else {
				s3restore.RestoreState = ActionStateRestoreFailed
			}

			objectMD.RestoreStatus = &s3restore

			//log.Logf("Restore state is : %v\n", object.RestoreStatus.RestoreState)

			res, err := s.s3Client.UpdateObject(ctx, objectMD)
			if err != nil {
				log.Logf("object metadata restore updated successfully and response is : %v\n", res)
			}

			log.Logf("restore object %s successful.", objectKey)
			response.WriteEntity(res)
		} else {
			log.Logf("specified object does not exist")
			ErrStr := strings.Replace(NoObjectExist, "$1", objectKey, 1)
			response.WriteError(http.StatusBadRequest, fmt.Errorf(ErrStr))
			return
		}

	} else {
		log.Log("no request body provided for restoring object")
		response.WriteError(http.StatusBadRequest, fmt.Errorf(NoRequestBodyRestoreObject))
		return
	}
}
