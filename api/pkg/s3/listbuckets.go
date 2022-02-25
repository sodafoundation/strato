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
	"time"

	"github.com/emicklei/go-restful"
	log "github.com/sirupsen/logrus"

	"github.com/opensds/multi-cloud/api/pkg/common"
	"github.com/opensds/multi-cloud/api/pkg/filters/signature"
	. "github.com/opensds/multi-cloud/api/pkg/s3/datatype"
	"github.com/opensds/multi-cloud/s3/pkg/utils"
	s3 "github.com/opensds/multi-cloud/s3/proto"
)

func parseListBuckets(list *s3.ListBucketsResponse) ListBucketsResponse {
	resp := ListBucketsResponse{}

	log.Infof("Parse ListBuckets ....todo some adding messg for testing: %v", list.Buckets)
	buckets := []Bucket{}
	for _, value := range list.Buckets {
		ctime := time.Unix(value.CreateTime, 0).Format(timeFormatAMZ)
		versionOpts := VersioningConfiguration{}
		versionOpts.Status = utils.VersioningDisabled
		if value.Versioning != nil {
			if value.Versioning.Status == utils.VersioningEnabled {
				versionOpts.Status = utils.VersioningEnabled
			}
		}
		sseOpts := SSEConfiguration{}
		if value.ServerSideEncryption != nil {
			if value.ServerSideEncryption.SseType == "SSE" {
				sseOpts.SSE.Enabled = "true"
			} else {
				sseOpts.SSE.Enabled = "false"
			}
		}
		bucket := Bucket{Name: value.Name, CreationDate: ctime, LocationConstraint: value.DefaultLocation,
			VersionOpts: versionOpts, SSEOpts: sseOpts}
		buckets = append(buckets, bucket)
	}
	resp.Buckets.Buckets = buckets

	return resp
}

func (s *APIService) ListBuckets(request *restful.Request, response *restful.Response) {
	log.Infof("Received request for all buckets")
	err := signature.PayloadCheck(request, response)
	if err != nil {
		WriteErrorResponse(response, request, err)
		return
	}

	ctx := common.InitCtxWithAuthInfo(request)
	rsp, err := s.s3Client.ListBuckets(ctx, &s3.ListBucketsRequest{})
	if HandleS3Error(response, request, err, rsp.GetErrorCode()) != nil {
		log.Errorf("list bucket failed, err=%v, errCode=%d\n", err, rsp.GetErrorCode())
		return
	}

	resp := parseListBuckets(rsp)
	resp.Owner = Owner{}
	resp.Owner.ID = common.GetOwner(request)

	// Encode response
	encodedResp := EncodeResponse(resp)
	// write response
	WriteSuccessResponse(response, encodedResp)
	log.Infoln("List buckets successfully.")
}
