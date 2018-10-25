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
	"bytes"
	"net/http"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"

	//	"github.com/micro/go-micro/errors"
	"strconv"

	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	"github.com/opensds/multi-cloud/s3/proto"
	"golang.org/x/net/context"
)

//ObjectGet -
func (s *APIService) ObjectGet(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	objectKey := request.PathParameter("objectKey")
	contentLenght := request.HeaderParameter("content-length")

	ctx := context.WithValue(request.Request.Context(), "operation", "download")

	log.Logf("Received request for create bucket: %s", bucketName)

	objectInput := s3.GetObjectInput{Bucket: bucketName, Key: objectKey}
	objectMD, _ := s.s3Client.GetObject(ctx, &objectInput)
	if objectMD != nil {
		//TODO update

	} else {

	}

	object := s3.Object{}
	object.ObjectKey = objectKey
	object.BucketName = bucketName
	size, _ := strconv.ParseInt(contentLenght, 10, 64)
	object.Size = size

	client := getBackendClient(s, bucketName)
	if client == nil {
		response.WriteError(http.StatusInternalServerError, NoSuchBackend.Error())
		return
	}

	body, s3err := client.GET(&object, ctx)

	if s3err != NoError {
		response.WriteError(http.StatusInternalServerError, s3err.Error())
		return
	}
	buf := new(bytes.Buffer)
	buf.ReadFrom(body)
	log.Logf("Download succeed, body:%v\n", buf.String())
	response.WriteEntity(buf.String())

}
