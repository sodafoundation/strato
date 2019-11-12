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
	"strings"
	"time"

	"github.com/emicklei/go-restful"
	"github.com/opensds/multi-cloud/api/pkg/common"
	c "github.com/opensds/multi-cloud/api/pkg/context"
	"github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	"github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

func (s *APIService) BucketPut(request *restful.Request, response *restful.Response) {
	bucketName := strings.ToLower(request.PathParameter(common.REQUEST_PATH_BUCKET_NAME))
	if !isValidBucketName(bucketName) {
		WriteErrorResponse(response, request, s3error.ErrInvalidBucketName)
		return
	}
	log.Infof("received request: PUT bucket[name=%s]\n", bucketName)

	if len(request.HeaderParameter(common.REQUEST_HEADER_CONTENT_LENGTH)) == 0 {
		log.Errorf("missing content length")
		WriteErrorResponse(response, request, s3error.ErrMissingContentLength)
		return
	}

	ctx := common.InitCtxWithAuthInfo(request)
	actx := request.Attribute(c.KContext).(*c.Context)
	bucket := s3.Bucket{Name: bucketName}
	bucket.TenantId = actx.TenantId
	bucket.UserId = actx.UserId
	bucket.Deleted = false
	bucket.CreateTime = time.Now().Unix()
	log.Infof("Bucket PUT: TenantId=%s, UserId=%s\n", bucket.TenantId, bucket.UserId)

	body := ReadBody(request)
	flag := false
	if body != nil && len(body) != 0 {
		log.Infof("request body is not empty")
		createBucketConf := model.CreateBucketConfiguration{}
		err := xml.Unmarshal(body, &createBucketConf)
		if err != nil {
			log.Infof("unmarshal failed, body:%v, err:%v\n", body, err)
			WriteErrorResponse(response, request, s3error.ErrUnmarshalFailed)
			return
		}

		backendName := createBucketConf.LocationConstraint
		if backendName != "" {
			log.Infof("backendName is %v\n", backendName)
			bucket.DefaultLocation = backendName
			flag = s.isBackendExist(ctx, backendName)
		}
	}
	if flag == false {
		log.Errorf("default backend is not provided or it is not exist.")
		WriteErrorResponse(response, request, s3error.ErrGetBackendFailed)
		return
	}

	rsp, err := s.s3Client.CreateBucket(ctx, &bucket)
	if HandleS3Error(response, request, err, rsp.ErrorCode) != nil {
		log.Errorf("delete bucket[%s] failed, err=%v, errCode=%d\n", bucketName, err, rsp.ErrorCode)
		return
	}

	log.Infof("create bucket[name=%s, defaultLocation=%s] successfully.\n", bucket.Name, bucket.DefaultLocation)
	// Make sure to add Location information here only for bucket
	response.Header().Set("Location", GetLocation(request.Request))
	WriteSuccessResponse(response, nil)
}
