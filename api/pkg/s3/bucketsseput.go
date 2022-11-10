// Copyright 2019 The soda Authors.
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
	log "github.com/sirupsen/logrus"

	"github.com/soda/multi-cloud/api/pkg/common"
	"github.com/soda/multi-cloud/s3/pkg/model"
	s3 "github.com/soda/multi-cloud/s3/proto"
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
	if bucket.ErrorCode != 0 {
		log.Errorf("get bucket failed, err=%v\n", bucket.ErrorCode)
		response.WriteError(http.StatusInternalServerError, fmt.Errorf("Get bucket error code %v", bucket.ErrorCode))
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

		s3SSE := &s3.ServerSideEncryption{
			SseType:              "",
			EncryptionKey:        nil,
			InitilizationVector:  nil,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		}
		if sseConf.SSE.Enabled == "true" {
			s3SSE.SseType = "SSE"
		}

		bucket.BucketMeta.ServerSideEncryption = s3SSE

		baseResponse, errSSE := s.s3Client.UpdateBucket(ctx, bucket.BucketMeta)
		if baseResponse.ErrorCode != 0 {
			response.WriteError(http.StatusInternalServerError, fmt.Errorf("Update bucket SSE options failed, error code %v", baseResponse.ErrorCode))
		}
		if errSSE != nil {
			response.WriteError(http.StatusInternalServerError, fmt.Errorf("Update bucket SSE options failed"))
		}

	} else {
		log.Info("no request body provided for creating SSE configuration")
		response.WriteError(http.StatusBadRequest, fmt.Errorf(NoRequestBodySSE))
		return
	}

	// Create bucket with bucket name will check if the bucket exists or not, if it exists
	// it will internally call UpdateBucket function
	res, err := s.s3Client.UpdateBucket(ctx, bucket.BucketMeta)
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	log.Info("create bucket SSE successful.")
	response.WriteEntity(res)

}
