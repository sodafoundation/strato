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
	"strconv"
	"strings"
	"time"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"
	"github.com/opensds/multi-cloud/api/pkg/common"
	c "github.com/opensds/multi-cloud/api/pkg/context"
	"github.com/opensds/multi-cloud/api/pkg/s3/datastore"
	"github.com/opensds/multi-cloud/api/pkg/utils/constants"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	"github.com/opensds/multi-cloud/s3/pkg/utils"
	"github.com/opensds/multi-cloud/s3/proto"
)

//ObjectPut -
func (s *APIService) ObjectPut(request *restful.Request, response *restful.Response) {
	url := request.Request.URL
	log.Logf("URL is %v", request.Request.URL.String())

	bucketName := request.PathParameter("bucketName")
	objectKey := request.PathParameter("objectKey")
	if strings.HasSuffix(url.String(), "/") {
		objectKey = objectKey + "/"
	}

	contentLenght := request.HeaderParameter("content-length")
	size, err := strconv.ParseInt(contentLenght, 10, 64)
	if err != nil {
		log.Logf("get content length failed, err: %v\n", err)
		response.WriteError(http.StatusInternalServerError, InvalidContentLength.Error())
		return
	}
	backendName := request.HeaderParameter("x-amz-storage-class")
	log.Logf("object.size is %v, objectKey is %s, backend name is%s\n", size, objectKey, backendName)

	// Currently, only support tier1 as default
	tier := int32(utils.Tier1)

	object := s3.Object{}
	object.BucketName = bucketName
	object.ObjectKey = objectKey
	object.Size = size
	object.IsDeleteMarker = ""
	object.InitFlag = ""
	object.LastModified = time.Now().Unix()
	object.Tier = tier
	// standard as default
	object.StorageClass = constants.StorageClassOpenSDSStandard

	md := map[string]string{common.REST_KEY_OPERATION: common.REST_VAL_UPLOAD}
	ctx := common.InitCtxWithVal(request, md)
	actx := request.Attribute(c.KContext).(*c.Context)
	object.UserId = actx.UserId
	object.TenantId = actx.TenantId

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
	log.Logf("enter the PUT method")
	s3err := client.PUT(request.Request.Body, &object, ctx)
	log.Logf("LastModified is %v\n", object.LastModified)
	if s3err != NoError {
		response.WriteError(http.StatusInternalServerError, s3err.Error())
		return
	}

	res, err := s.s3Client.CreateObject(ctx, &object)
	if err != nil {
		log.Logf("err is %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	log.Logf("object.size2  = %v \n", object.Size)
	log.Log("Upload object successfully.")
	response.WriteEntity(res)
}
