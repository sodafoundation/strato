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
	"github.com/opensds/multi-cloud/api/pkg/policy"
	//"github.com/micro/go-log"
)

func (s *APIService) RouteBucketGet(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "routbucket:get") {
		return
	}
	if IsQuery(request, "acl") {
		//TODO
	} else if IsQuery(request, "versioning") {
		//TODO
	} else if IsQuery(request, "website") {
		//TODO
	} else if IsQuery(request, "cors") {
		//TODO

	} else if IsQuery(request, "replication") {
		//TODO

	} else if IsQuery(request, "lifecycle") {
		s.BucketLifecycleGet(request, response)

	} else {
		s.BucketGet(request, response)
	}
}
