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
	"net/http"

	"github.com/emicklei/go-restful"
	"github.com/opensds/multi-cloud/api/pkg/common"
	"github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

func (s *APIService) BucketDelete(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	log.Infof("Received request for deleting bucket[name=%s].\n", bucketName)

	ctx := common.InitCtxWithAuthInfo(request)
	_, err := s.s3Client.DeleteBucket(ctx, &s3.Bucket{Name: bucketName})
	if err != nil {
		if err.Error() == s3error.ErrNoSuchBucket.Error() {
			response.WriteError(http.StatusNotFound, err)
		} else {
			response.WriteError(http.StatusInternalServerError, err)
		}
		log.Errorf("delete bucket failed, err:%v\n", err)
		return
	}
}
