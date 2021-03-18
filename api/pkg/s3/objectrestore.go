// Copyright 2020 The OpenSDS Authors.
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
	"strings"

	"github.com/emicklei/go-restful"
	"github.com/opensds/multi-cloud/api/pkg/common"
	"github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	"github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

//Object Restore from Archival storage
func (s *APIService) RestoreObject(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter(common.REQUEST_PATH_BUCKET_NAME)
	objectKey := request.PathParameter(common.REQUEST_PATH_OBJECT_KEY)

	url := request.Request.URL
	if strings.HasSuffix(url.String(), "/") {
		objectKey = objectKey + "/"
	}
	log.Debugf("Received request: restore object, objectkey=%s, bucketName=%s [%v]:",
		objectKey, bucketName, request)

	var err error
	if !isValidObjectName(objectKey) {
		log.Errorf("invalid object name")
		WriteErrorResponse(response, request, s3error.ErrInvalidObjectName)
		return
	}

	// check if specific bucket exist
	ctx := common.InitCtxWithAuthInfo(request)

	restoreObjDetail := &model.Restore{}
	err = request.ReadEntity(&restoreObjDetail)
	if err != nil {
		log.Errorf("failed to invoke restore object: %v", err)
		WriteErrorResponse(response, request, err)
		return
	}

	resObj := &s3.Restore{
		Days:         restoreObjDetail.Days,
		Tier:         restoreObjDetail.Tier,
		StorageClass: restoreObjDetail.StorageClass,
		BucketName:   bucketName,
		ObjectKey:    objectKey,
	}

	res, restoreErr := s.s3Client.RestoreObject(ctx, &s3.RestoreObjectRequest{Restore: resObj})
	if err != nil {
		log.Errorf("failed to restore object %v", restoreErr)
		WriteErrorResponse(response, request, restoreErr)
		return
	}

	log.Info("POST restore object successfully.[%v]", res)
	WriteSuccessResponse(response, nil)
}
