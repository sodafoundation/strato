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
	"github.com/emicklei/go-restful"
	"github.com/opensds/multi-cloud/api/pkg/common"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	"github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
	"net/http"
)

func (s *APIService) BucketDelete(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	log.Infof("Received request for bucket details: %s", bucketName)

	ctx := common.InitCtxWithAuthInfo(request)
	res, err := s.s3Client.ListObjects(ctx, &s3.ListObjectsRequest{Bucket: bucketName})

	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	if len(res.ListObjects) == 0 {
		res1, err1 := s.s3Client.DeleteBucket(ctx, &s3.Bucket{Name: bucketName})
		if err1 != nil {
			response.WriteError(http.StatusInternalServerError, err1)
			return
		}
		log.Info("Delete bucket successfully.")
		response.WriteEntity(res1)
	} else {
		log.Info("The bucket can not be deleted. please delete objects first.\n")
		response.WriteError(http.StatusInternalServerError, BucketDeleteError.Error())
	}
}
