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
	"encoding/xml"
	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"
	"net/http"
	//	"github.com/micro/go-micro/errors"
	"github.com/opensds/multi-cloud/s3/proto"
	"golang.org/x/net/context"
	"time"
)

func (s *APIService) BucketPut(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")

	log.Logf("Received request for create bucket: %s", bucketName)
	ctx := context.Background()

	bucket := s3.Bucket{Name: bucketName}
	body := ReadBody(request)
	//TODO owner
	owner := "test"
	ownerDisplayName := "test"
	bucket.Owner = owner
	bucket.OwnerDisplayName = ownerDisplayName
	bucket.CreationDate = time.Now().Unix()
	log.Logf("Create bucket body: %s", string(body))
	if body != nil {
		createBucketConf := CreateBucketConfiguration{}
		err := xml.Unmarshal(body, &createBucketConf)
		if err != nil {
			response.WriteError(http.StatusInternalServerError, err)
			return
		} else {
			bucket.Backend = createBucketConf.LocationConstraint
		}
	}

	res, err := s.s3Client.CreateBucket(ctx, &bucket)
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	log.Log("Create bucket successfully.")
	response.WriteEntity(res)

}
