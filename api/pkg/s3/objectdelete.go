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
	"errors"
	"github.com/emicklei/go-restful"
	"github.com/opensds/multi-cloud/api/pkg/common"
	"github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
	"net/http"
	"strings"
)

func (s *APIService) ObjectDelete(request *restful.Request, response *restful.Response) {
	url := request.Request.URL
	bucketName := request.PathParameter("bucketName")
	objectName := request.PathParameter("objectKey")
	version := url.Query().Get("versionId")
	if strings.HasSuffix(url.String(), "/") {
		objectName = objectName + "/"
	}

	if len(bucketName) == 0 || len(objectName) == 0 {
		log.Errorf("invalid input, bucket=%s, object=%s\n", bucketName, objectName)
		response.WriteError(http.StatusBadRequest, errors.New("invalid bucket or object"))
	}

	input := s3.DeleteObjectInput{Bucket: bucketName, Key: objectName}
	if len(version) > 0 {
		input.VersioId = version
	}

	ctx := common.InitCtxWithAuthInfo(request)
	rsp, err := s.s3Client.DeleteObject(ctx, &input)
	if err != nil {
		log.Errorf("delete object[%s] failed: %v\n", objectName, err)
		WriteErrorResponseWithErrCode(response, rsp.ErrorCode, err)
		return
	}
	if rsp.DeleteMarker {
		response.Header().Set("x-amz-delete-marker", "true")
	} else {
		response.Header().Set("x-amz-delete-marker", "false")
	}
	if rsp.VersionId != "" {
		response.Header().Set("x-amz-version-id", rsp.VersionId)
	}

	log.Infof("delete object[%s] from bucket[%s] succeed.", objectName, bucketName)
	response.WriteHeader(http.StatusNoContent)
}
