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
	"net/http"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"

	//	"github.com/micro/go-micro/errors"
	"strconv"

	"encoding/xml"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	"github.com/opensds/multi-cloud/s3/proto"
	"golang.org/x/net/context"
)

//ObjectPut -
func (s *APIService) MultiPartUploadInit(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	objectKey := request.PathParameter("objectKey")
	contentLenght := request.HeaderParameter("content-length")

	ctx := context.WithValue(request.Request.Context(), "operation", "multipartupload")

	log.Logf("Received request for create bucket: %s", bucketName)

	object := s3.Object{}
	object.ObjectKey = objectKey
	object.BucketName = bucketName
	multipartUpload := s3.MultipartUpload{}
	multipartUpload.Bucket = bucketName
	multipartUpload.Key = objectKey
	size, _ := strconv.ParseInt(contentLenght, 10, 64)
	object.Size = size

	client := getBackendClient(s, bucketName)
	if client == nil {
		response.WriteError(http.StatusInternalServerError, NoSuchBackend.Error())
		return
	}
	res, s3err := client.InitMultipartUpload(&object, ctx)
	if s3err != NoError {
		response.WriteError(http.StatusInternalServerError, s3err.Error())
		return
	}
	result := model.InitiateMultipartUploadResult{
		Xmlns:    model.Xmlns,
		Bucket:   res.Bucket,
		Key:      res.Key,
		UploadId: res.UploadId,
	}

	xmlstring, err := xml.MarshalIndent(result, "", "  ")
	if err != nil {
		log.Logf("Parse ListBuckets error: %v", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	xmlstring = []byte(xml.Header + string(xmlstring))
	log.Logf("resp:\n%s", xmlstring)
	response.Write(xmlstring)
	log.Log("Uploadpart successfully.")
}
