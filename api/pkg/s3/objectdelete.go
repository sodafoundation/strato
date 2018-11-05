// Copyright (c) 2018 Huawei Technologies Co., Ltd. All Rights Reserved.
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
	"github.com/micro/go-log"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	"github.com/opensds/multi-cloud/s3/proto"
	"golang.org/x/net/context"
	"net/http"
	//	"github.com/micro/go-micro/errors"
)

func (s *APIService) ObjectDelete(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	objectKey := request.PathParameter("objectKey")
	deleteInput := s3.DeleteObjectInput{Key: objectKey, Bucket: bucketName}
	ctx := context.Background()
	log.Logf("Received request for delete object: %s", objectKey)

	objectInput := s3.GetObjectInput{Bucket: bucketName, Key: objectKey}
	objectMD, _ := s.s3Client.GetObject(ctx, &objectInput)
	var s3err S3Error
	if objectMD != nil {
		client := getBackendByName(s, objectMD.Backend)
		s3err = client.DELETE(&deleteInput, ctx)
	}else{
		log.Logf("No such object")
		return
	}

	if s3err.Code != ERR_OK {
		response.WriteError(http.StatusInternalServerError, s3err.Error())
		return
	}

	res, err := s.s3Client.DeleteObject(ctx, &deleteInput)
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	log.Logf("Delete object %s successfully.", objectKey)
	response.WriteEntity(res)
}
