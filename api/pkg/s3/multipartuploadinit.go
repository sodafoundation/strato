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
	"encoding/xml"
	"net/http"
	"time"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"
	"github.com/opensds/multi-cloud/api/pkg/common"
	c "github.com/opensds/multi-cloud/api/pkg/context"
	"github.com/opensds/multi-cloud/api/pkg/s3/datastore"
	"github.com/opensds/multi-cloud/api/pkg/utils/constants"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	"github.com/opensds/multi-cloud/s3/pkg/utils"
	"github.com/opensds/multi-cloud/s3/proto"
	"golang.org/x/net/context"
)

//ObjectPut -
func (s *APIService) MultiPartUploadInit(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	objectKey := request.PathParameter("objectKey")
	log.Logf("Received request for multi-part upload init, bucket: %s, object: %s\n", bucketName, objectKey)

	md := map[string]string{common.REST_KEY_OPERATION: common.REST_VAL_MULTIPARTUPLOAD}
	ctx := common.InitCtxWithVal(request, md)
	actx := request.Attribute(c.KContext).(*c.Context)
	//assign backend
	backendName := request.HeaderParameter("x-amz-storage-class")
	log.Logf("backendName is %v\n", backendName)

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
		client = getBackendByName(ctx, s, backendName)
	} else {
		bucket, _ := s.s3Client.GetBucket(ctx, &s3.Bucket{Name: bucketName})
		object.Backend = bucket.Backend
		client = getBackendClient(ctx, s, bucketName)
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

	lastModified := time.Now().Unix()
	record := s3.MultipartUploadRecord{ObjectKey: objectKey, Bucket: bucketName, Backend: object.Backend, UploadId: res.UploadId}
	record.InitTime = lastModified
	record.TenantId = actx.TenantId
	record.UserId = actx.UserId
	_, err := s.s3Client.AddUploadRecord(context.Background(), &record)
	if err != nil {
		client.AbortMultipartUpload(res, ctx)
		response.WriteError(http.StatusInternalServerError, s3err.Error())
		return
	}

	// Currently, only support tier1 as default
	tier := int32(utils.Tier1)
	object.Tier = tier
	// standard as default
	object.StorageClass = constants.StorageClassOpenSDSStandard

	object.ObjectKey = objectKey
	objectInput := s3.GetObjectInput{Bucket: bucketName, Key: objectKey}
	objectMD, _ := s.s3Client.GetObject(ctx, &objectInput)
	if objectMD != nil {
		objectMD.ObjectKey = objectKey
		objectMD.BucketName = bucketName
		objectMD.InitFlag = "0"
		objectMD.IsDeleteMarker = ""
		objectMD.Partions = nil
		objectMD.Backend = object.Backend
		objectMD.Size = int64(size)
		objectMD.LastModified = lastModified
		objectMD.Tier = object.Tier
		objectMD.StorageClass = object.StorageClass
		//insert metadata
		_, err := s.s3Client.CreateObject(ctx, objectMD)
		if err != nil {
			log.Logf("err is %v\n", err)
			response.WriteError(http.StatusInternalServerError, err)
			client.AbortMultipartUpload(res, ctx)
			return
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
			client.AbortMultipartUpload(res, ctx)
			return
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
		client.AbortMultipartUpload(res, ctx)
		return
	}

	xmlstring = []byte(xml.Header + string(xmlstring))
	log.Logf("resp:\n%s", xmlstring)
	response.Write(xmlstring)
	log.Log("Uploadpart successfully.")
}
