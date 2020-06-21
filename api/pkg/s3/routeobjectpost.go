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
	"regexp"

	"github.com/emicklei/go-restful"
	"github.com/opensds/multi-cloud/api/pkg/common"
	"github.com/opensds/multi-cloud/api/pkg/filters/signature"
)

const (
	TypePostFormData int = iota
	TypeInitMultipartUpload
	TypeCompleteMultipartUpload
)

func (s *APIService) RouteObjectPost(request *restful.Request, response *restful.Response) {
	requestType := TypePutObj
	if IsQuery(request, "uploads") {
		requestType = TypeInitMultipartUpload
	} else if IsQuery(request, "uploadId") {
		requestType = TypeCompleteMultipartUpload
	}

	// Post object operation no need content check.
	if requestType == TypeInitMultipartUpload || requestType == TypeCompleteMultipartUpload {
		err := signature.PayloadCheck(request, response)
		if err != nil {
			WriteErrorResponse(response, request, err)
			return
		}
	}

	switch requestType {
	case TypeInitMultipartUpload:
		s.MultiPartUploadInit(request, response)
	case TypeCompleteMultipartUpload:
		s.CompleteMultipartUpload(request, response)
	default:
		// Supposed to be post object operation, check validation.
		contentType := request.HeaderParameter(common.REQUEST_HEADER_CONTENT_TYPE)
		objectPostValidate := regexp.MustCompile("multipart/form-data*")
		if contentType != "" && objectPostValidate.MatchString(contentType) {
			s.ObjectPost(request, response)
			return
		}
	}
}
