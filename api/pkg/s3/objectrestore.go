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
	"net/http"
	"strings"

	"github.com/emicklei/go-restful"
	log "github.com/sirupsen/logrus"

	"github.com/opensds/multi-cloud/api/pkg/common"
	backend "github.com/opensds/multi-cloud/backend/proto"
	s3error "github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	s3 "github.com/opensds/multi-cloud/s3/proto"
)

//FIXME: This const values are declared for tiering feature. Later this can be read from config file
const (
	AWS_TYPE                    = "aws-s3"
	AZURE_TYPE                  = "azure-blob"
	GCP_TYPE                    = "gcp-s3"
	AWS_DEFAULT_RESTORE_DAYS    = 5
	AWS_DEFAULT_RESTORE_TIER    = "Expedited"
	AZURE_DEFAULT_RESTORE_CLASS = "Hot"
	AWS_GLACIER                 = "GLACIER"
	ARCHIVE                     = "Archive"
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

	// if ssp is enabled, get the bucketMeta data to get all the details
	// like backend, backend type, storageClass etc.
	bucketMeta, err := s.getBucketMeta(ctx, bucketName)
	if err != nil {
		log.Errorln("failed to get bucket meta. err:", err)
		WriteErrorResponse(response, request, err)
		return
	}

	if bucketMeta.Ssps != "" {
		adminCtx := common.GetAdminContext()
		backendId := s.GetBackendIdFromSsp(adminCtx, bucketMeta.Ssps)
		backendMeta, err := s.backendClient.GetBackend(adminCtx, &backend.GetBackendRequest{Id: backendId})
		if err != nil {
			log.Error("the selected backends from ssp doesn't exists.")
			response.WriteError(http.StatusInternalServerError, err)
		}
		if backendMeta != nil {
			backendType := backendMeta.Backend.Type

			if backendType == AWS_TYPE {
				restoreObjDetail.Days = AWS_DEFAULT_RESTORE_DAYS
				restoreObjDetail.Tier = AWS_DEFAULT_RESTORE_TIER
			} else if backendType == AZURE_TYPE {
				restoreObjDetail.StorageClass = AZURE_DEFAULT_RESTORE_CLASS
			}
		}
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
