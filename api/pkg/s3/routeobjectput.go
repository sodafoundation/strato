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
	log "github.com/sirupsen/logrus"

	"github.com/opensds/multi-cloud/api/pkg/filters/signature"
	s3error "github.com/opensds/multi-cloud/s3/error"
)

const (
	TypePutObj int = iota
	TypePutACL
	TypeCopyObj
	TypeCopyPart
	TypeUploadPart
	TypeTagging
)

func (s *APIService) RouteObjectPut(request *restful.Request, response *restful.Response) {
	requestType := TypePutObj
	if IsQuery(request, "acl") {
		requestType = TypePutACL
	} else if IsQuery(request, "tagging") {
		requestType = TypeTagging
	} else if IsQuery(request, "partNumber") && IsQuery(request, "uploadId") &&
		HasHeader(request, "x-amz-copy-source") {
		requestType = TypeCopyPart
	} else if IsQuery(request, "partNumber") && IsQuery(request, "uploadId") {
		requestType = TypeUploadPart
	} else if HasHeader(request, "x-amz-copy-source") {
		requestType = TypeCopyObj
	}

	// For TypePutObj and TypeUploadPart, will check payload later.
	if requestType != TypePutObj && requestType != TypeUploadPart {
		err := signature.PayloadCheck(request, response)
		if err != nil {
			WriteErrorResponse(response, request, err)
			return
		}
	}

	switch requestType {
	case TypePutACL:
		s.ObjectAclPut(request, response)
	case TypeCopyPart:
		s.ObjectPartCopy(request, response)
	case TypeUploadPart:
		s.UploadPart(request, response)
	case TypeCopyObj:
		s.ObjectCopy(request, response)
	case TypePutObj:
		s.ObjectPut(request, response)
	default:
		// Not support
		log.Errorln("not implemented")
		WriteErrorResponse(response, request, s3error.ErrNotImplemented)
	}
}
