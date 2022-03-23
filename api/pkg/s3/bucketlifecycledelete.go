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
	"github.com/emicklei/go-restful"
	log "github.com/sirupsen/logrus"

	"github.com/soda/multi-cloud/api/pkg/common"
	s3 "github.com/soda/multi-cloud/s3/proto"
)

func (s *APIService) BucketLifecycleDelete(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	log.Infof("received request for creating lifecycle of bucket: %s", bucketName)

	ctx := common.InitCtxWithAuthInfo(request)
	rsp, err := s.s3Client.DeleteBucketLifecycle(ctx, &s3.BaseRequest{Id: bucketName})
	log.Infof("rsp:%s, err:%v\n", rsp, err)
	if HandleS3Error(response, request, err, rsp.GetErrorCode()) != nil {
		log.Errorf("delete bucket[%s] lifecycle failed, err=%v, errCode=%d\n", bucketName, err, rsp.GetErrorCode())
		return
	}

	log.Info("delete bucket lifecycle end.")
}
