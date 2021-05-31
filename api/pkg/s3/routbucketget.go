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
	"github.com/emicklei/go-restful"

	"github.com/opensds/multi-cloud/api/pkg/filters/signature"
	. "github.com/opensds/multi-cloud/s3/error"
)

func (s *APIService) RouteBucketGet(request *restful.Request, response *restful.Response) {
	err := signature.PayloadCheck(request, response)
	if err != nil {
		WriteErrorResponse(response, request, err)
		return
	}
	if IsQuery(request, "acl") {
		s.BucketAclGet(request, response)
	} else if IsQuery(request, "uploads") {
		s.ListBucketUploadRecords(request, response)
	} else if IsQuery(request, "versions") {
		//TODO
		WriteErrorResponse(response, request, ErrNotImplemented)
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
