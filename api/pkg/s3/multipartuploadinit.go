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
	"github.com/opensds/multi-cloud/api/pkg/s3/datastore"
	"net/http"
	"time"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"

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
	//assign backend
	backendName := request.HeaderParameter("x-amz-storage-class")
	ctx := context.WithValue(request.Request.Context(), "operation", "multipartupload")

	log.Logf("Received request for create bucket: %s", bucketName)
	size := 0
	object := s3.Object{}
	object.ObjectKey = objectKey
	object.BucketName = bucketName
	multipartUpload := s3.MultipartUpload{}
	multipartUpload.Bucket = bucketName
	multipartUpload.Key = objectKey

	var client datastore.DataStoreAdapter
	if backendName != "" {
		object.Backend = backendName
		client = getBackendByName(s, backendName)
	} else {
		bucket, _ := s.s3Client.GetBucket(ctx, &s3.Bucket{Name: bucketName})
		object.Backend = bucket.Backend
		client = getBackendClient(s, bucketName)
	}
	if client == nil {
		response.WriteError(http.StatusInternalServerError, NoSuchBackend.Error())
		return
	}
	res, s3err := client.InitMultipartUpload(&object, ctx)
	if s3err != NoError {
		response.WriteError(http.StatusInternalServerError, s3err.Error())
		return
	}
	lastModified := time.Now().String()[:19]
	objectInput := s3.GetObjectInput{Bucket: bucketName, Key: objectKey}
	objectMD, _ := s.s3Client.GetObject(ctx, &objectInput)
	if objectMD != nil {
		objectMD.ObjectKey = objectKey
		objectMD.BucketName = bucketName
		objectMD.InitFlag = "0"
		objectMD.IsDeleteMarker = ""
		objectMD.Partions = nil
		objectMD.Size = int64(size)
		objectMD.LastModified = lastModified
		//insert metadata
		_, err := s.s3Client.CreateObject(ctx, objectMD)
		if err != nil {
			log.Logf("err is %v\n", err)
			response.WriteError(http.StatusInternalServerError, err)
		}
	} else {
		object.Size = int64(size)
		object.LastModified = lastModified
		object.InitFlag = "0"

		//insert metadata
		_, err := s.s3Client.CreateObject(ctx, &object)
		if err != nil {
			log.Logf("err is %v\n", err)
			response.WriteError(http.StatusInternalServerError, err)
		}
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
