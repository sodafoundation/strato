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
	"github.com/opensds/multi-cloud/api/pkg/common"
	. "github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	s3 "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

func (s *APIService) BucketVersioningPut(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	log.Infof("received request for creating versioning of bucket: %s", bucketName)

	ctx := common.InitCtxWithAuthInfo(request)
	bucket, err := s.s3Client.GetBucket(ctx, &s3.Bucket{Name: bucketName})
	if HandleS3Error(response, request, err, bucket.ErrorCode) != nil {
		log.Errorf("get bucket[%s] lifecycle failed, err=%v, errCode=%d\n", bucketName, err, bucket.ErrorCode)
		return
	}

	body := ReadBody(request)
	log.Infof("MD5 sum for body is %x", md5.Sum(body))
	if body == nil {
		log.Info("no request body provided for creating versioning configuration")
		WriteErrorResponse(response, request, S3ErrorCode(ErrInvalidVersion))
		return
	}

	versionConf := model.VersioningConfiguration{}
	err = xml.Unmarshal(body, &versionConf)
	if err != nil {
		WriteErrorResponse(response, request, S3ErrorCode(ErrInvalidVersion))
		return
	}

	s3version := &s3.BucketVersioning{
		Status:               "",
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}

	if versionConf.Status == "Enabled"{
		s3version.Status = "Enabled"
	} else{
		s3version.Status = "Disabled"
	}

	bucket.BucketMeta.Versioning = s3version

	rsp, err := s.s3Client.CreateBucket(ctx, bucket.BucketMeta)
	if HandleS3Error(response, request, err, rsp.ErrorCode) != nil {
		log.Errorf("versioning configuration failed, err=%v, errCode=%d\n", bucketName, err, rsp.ErrorCode)
		return
	}

	log.Info("create bucket version configuration successful.")
	WriteSuccessResponse(response, nil)
}
