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
	"github.com/opensds/multi-cloud/api/pkg/policy"
)

func (s *APIService) RouteObjectPut(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "object:put") {
		return
	}

	if IsQuery(request, "acl") {
		//TODO
	} else if IsQuery(request, "tagging") {
		//TODO
	} else if IsQuery(request, "uploads") {
		s.MultiPartUploadInit(request, response)
	} else if IsQuery(request, "partNumber") && IsQuery(request, "uploadId") {
		s.UploadPart(request, response)
	} else if IsQuery(request, "uploadId") {
		s.CompleteMultipartUpload(request, response)
	} else if HasHeader(request, "x-amz-copy-source") {
	} else {
		s.ObjectPut(request, response)
	}
}
