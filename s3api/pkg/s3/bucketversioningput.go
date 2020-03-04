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
	"github.com/emicklei/go-restful"
	. "github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	"github.com/opensds/multi-cloud/s3/pkg/utils"
	s3 "github.com/opensds/multi-cloud/s3/proto"
	"github.com/opensds/multi-cloud/s3api/pkg/common"
	log "github.com/sirupsen/logrus"
)

func (s *APIService) BucketVersioningPut(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	log.Infof("received request for creating versioning of bucket: %s", bucketName)

	ctx := common.InitCtxWithAuthInfo(request)
	bucket, err := s.getBucketMeta(ctx, bucketName)
	if err != nil {
		WriteErrorResponse(response, request, err)
		log.Errorf("get bucket[%s] failed, err=%v\n", bucketName, err)
		return
	}

	body := ReadBody(request)
	if body == nil {
		log.Info("no request body provided for creating versioning configuration")
		WriteErrorResponse(response, request, S3ErrorCode(ErrInvalidVersioning))
		return
	}
	log.Infof("MD5 sum for body is %x", md5.Sum(body))

	versionConf := model.VersioningConfiguration{}
	err = xml.Unmarshal(body, &versionConf)
	if err != nil {
		WriteErrorResponse(response, request, S3ErrorCode(ErrInvalidVersioning))
		return
	}

	s3version := &s3.BucketVersioning{
		Status:               "",
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}

	if versionConf.Status == utils.VersioningEnabled {
		s3version.Status = utils.VersioningEnabled
	} else {
		s3version.Status = utils.VersioningDisabled
	}

	bucket.Versioning = s3version

	_, err = s.s3Client.UpdateBucket(ctx, bucket)
	if err != nil {
		log.Errorf("versioning configuration failed, errCode=%d\n", bucketName, err)
		return
	}

	log.Info("create bucket version configuration successful.")
	WriteSuccessResponse(response, nil)
}
