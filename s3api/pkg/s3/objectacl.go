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
	"io"
	"io/ioutil"
	"strings"

	"github.com/emicklei/go-restful"
	"github.com/opensds/multi-cloud/s3/error"
	pb "github.com/opensds/multi-cloud/s3/proto"
	"github.com/opensds/multi-cloud/s3api/pkg/common"
	"github.com/opensds/multi-cloud/s3api/pkg/s3/datatype"
	log "github.com/sirupsen/logrus"
)

func (s *APIService) ObjectAclPut(request *restful.Request, response *restful.Response) {
	bucketName := strings.ToLower(request.PathParameter(common.REQUEST_PATH_BUCKET_NAME))
	objectKey := request.PathParameter(common.REQUEST_PATH_OBJECT_KEY)
	log.Infof("received request: PUT bucket[name=%s] object[name=%s] acl\n", bucketName, objectKey)

	var err error
	var acl datatype.Acl
	var policy datatype.AccessControlPolicy
	if _, ok := request.Request.Header[common.REQUEST_HEADER_ACL]; ok {
		acl, err = getAclFromHeader(request)
		if err != nil {
			WriteErrorResponse(response, request, err)
			return
		}
	} else {
		// because we only support canned acl, the body of request must be not too big, and 1024 is enough
		aclBuffer, err := ioutil.ReadAll(io.LimitReader(request.Request.Body, 1024))
		if err != nil {
			log.Errorf("unable to read acls body, err:", err)
			WriteErrorResponse(response, request, s3error.ErrInvalidAcl)
			return
		}
		err = xml.Unmarshal(aclBuffer, &policy)
		if err != nil {
			log.Errorf("unable to parse acls xml body, err:", err)
			WriteErrorResponse(response, request, s3error.ErrInternalError)
			return
		}
	}

	if acl.CannedAcl == "" {
		newCannedAcl, err := datatype.GetCannedAclFromPolicy(policy)
		if err != nil {
			log.Error("failed to get canned acl from policy. err:", err)
			WriteErrorResponse(response, request, err)
			return
		}
		acl = newCannedAcl
	}

	ctx := common.InitCtxWithAuthInfo(request)
	res, err := s.s3Client.PutObjACL(ctx, &pb.PutObjACLRequest{ACLConfig: &pb.ObjACL{BucketName: bucketName,
		ObjectKey: objectKey, CannedAcl: acl.CannedAcl}})
	if err != nil || res.ErrorCode != int32(s3error.ErrNoErr) {
		WriteErrorResponse(response, request, GetFinalError(err, res.ErrorCode))
		return
	}

	log.Infoln("PUT object acl successfully.")
	WriteSuccessResponse(response, nil)
}

func (s *APIService) ObjectAclGet(request *restful.Request, response *restful.Response) {
	bucketName := strings.ToLower(request.PathParameter(common.REQUEST_PATH_BUCKET_NAME))
	objectKey := request.PathParameter(common.REQUEST_PATH_OBJECT_KEY)
	log.Infof("received request: GET bucket[name=%s] object[name=%s] acl\n", bucketName, objectKey)

	ctx := common.InitCtxWithAuthInfo(request)
	object, err := s.getObjectMeta(ctx, bucketName, objectKey, "")
	if err != nil {
		log.Error("failed to get object[%s] meta", objectKey)
		WriteErrorResponse(response, request, err)
		return
	}

	owner := datatype.Owner{ID: object.TenantId, DisplayName: object.TenantId}
	bucketOwner := datatype.Owner{}
	policy, err := datatype.CreatePolicyFromCanned(owner, bucketOwner, datatype.Acl{CannedAcl: object.Acl.CannedAcl})
	if err != nil {
		log.Error("failed to create policy. err:", err)
		WriteErrorResponse(response, request, err)
		return
	}

	aclBuffer, err := xmlFormat(policy)
	if err != nil {
		log.Errorf("failed to marshal acl XML for bucket, err:%v\n", bucketName, err)
		WriteErrorResponse(response, request, s3error.ErrInternalError)
		return
	}

	setXmlHeader(response, aclBuffer)
	WriteSuccessResponse(response, aclBuffer)
	log.Infoln("GET object acl successfully.")
}
