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
	"github.com/opensds/multi-cloud/api/pkg/policy"
	"net/http"
	"encoding/json"
	"fmt"
	"github.com/opensds/multi-cloud/api/pkg/common"
	//	"github.com/micro/go-micro/errors"
	"github.com/opensds/multi-cloud/s3/proto"
	"golang.org/x/net/context"
)

func (s *APIService) BucketLifecycleGet(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "bucket:get") {
		return
	}
	bucketName := request.PathParameter("bucketName")
	log.Logf("Received request for bucket details: %s", bucketName)

	filterOpts := []string{common.KObjKey, common.KLastModified}
	filter, err := common.GetFilter(request, filterOpts)
	if err != nil {
		log.Logf("Get filter failed: %v", err)
		response.WriteError(http.StatusBadRequest, err)
		return
	} else {
		log.Logf("Get filter for BucketGet, filterOpts=%+v, filter=%+v\n",
			filterOpts, filter)
	}

	if filter[common.KObjKey] != "" {
		//filter[common.KObjKey] should be like: like:parttern
		ret, err := checkObjKeyFilter(filter[common.KObjKey])
		if err != nil {
			log.Logf("Invalid objkey:%s\v", filter[common.KObjKey])
			response.WriteError(http.StatusBadRequest,
				fmt.Errorf("Invalid objkey, it should be like objkey=like:parttern"))
			return
		}
		filter[common.KObjKey] = ret
	}

	// Check validation of query parameter
	if filter[common.KLastModified] != "" {
		var tmFilter map[string]string
		err := json.Unmarshal([]byte(filter[common.KLastModified]), &tmFilter)
		if err != nil {
			log.Logf("Invalid lastModified:%s\v", filter[common.KLastModified])
			response.WriteError(http.StatusBadRequest,
				fmt.Errorf("Invalid lastmodified, it should be like lastmodified={\"lt\":\"numb\"}"))
			return
		}
		err = checkLastmodifiedFilter(&tmFilter)
		if err != nil {
			log.Logf("Invalid lastModified:%s\v", filter[common.KLastModified])
			response.WriteError(http.StatusBadRequest,
				fmt.Errorf("Invalid lastmodified, it should be like lastmodified={\"lt\":\"numb\"}"))
			return
		}
	}

	req := s3.ListObjectsRequest{
		Bucket: bucketName,
		Filter: filter,
	}

	ctx := context.Background()
	res, err := s.s3Client.ListObjects(ctx, &req)
	log.Logf("list objects result: %v\n", res)
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	log.Log("Get bucket successfully.")
	response.WriteEntity(res)
}
