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
	"github.com/opensds/multi-cloud/s3api/pkg/common"
	"github.com/opensds/multi-cloud/s3api/pkg/s3/datatype"
	"github.com/opensds/multi-cloud/s3/error"
	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

func (s *APIService) BucketAclPut(request *restful.Request, response *restful.Response) {
	bucketName := strings.ToLower(request.PathParameter(common.REQUEST_PATH_BUCKET_NAME))
	log.Infof("received request: PUT bucket[name=%s] acl\n", bucketName)

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
	res, err := s.s3Client.PutBucketACL(ctx, &pb.PutBucketACLRequest{ACLConfig: &pb.BucketACL{BucketName: bucketName, CannedAcl: acl.CannedAcl}})
	if err != nil || res.ErrorCode != int32(s3error.ErrNoErr) {
		WriteErrorResponse(response, request, GetFinalError(err, res.ErrorCode))
		return
	}

	log.Infoln("PUT bucket acl successfully.")
	WriteSuccessResponse(response, nil)
}

func (s *APIService) BucketAclGet(request *restful.Request, response *restful.Response) {
	bucketName := strings.ToLower(request.PathParameter(common.REQUEST_PATH_BUCKET_NAME))
	log.Infof("received request: GET bucket[name=%s] acl\n", bucketName)

	ctx := common.InitCtxWithAuthInfo(request)
	bucket, err := s.getBucketMeta(ctx, bucketName)
	if err != nil {
		log.Error("failed to get bucket[%s] acl policy for bucket", bucketName)
		WriteErrorResponse(response, request, err)
		return
	}

	owner := datatype.Owner{ID: bucket.TenantId, DisplayName: bucket.TenantId}
	bucketOwner := datatype.Owner{}
	policy, err := datatype.CreatePolicyFromCanned(owner, bucketOwner, datatype.Acl{CannedAcl: bucket.Acl.CannedAcl})
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
	log.Infoln("GET bucket acl successfully.")
}
