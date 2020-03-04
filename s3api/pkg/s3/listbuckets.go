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
	"github.com/opensds/multi-cloud/s3/proto"
	"github.com/opensds/multi-cloud/s3api/pkg/common"
	. "github.com/opensds/multi-cloud/s3api/pkg/s3/datatype"
	log "github.com/sirupsen/logrus"
)

func parseListBuckets(list *s3.ListBucketsResponse) ListBucketsResponse {
	resp := ListBucketsResponse{}

	log.Infof("Parse ListBuckets: %v", list.Buckets)
	buckets := []Bucket{}
	for _, value := range list.Buckets {
		ctime := time.Unix(value.CreateTime, 0).Format(timeFormatAMZ)
		bucket := Bucket{Name: value.Name, CreationDate: ctime, LocationConstraint: value.DefaultLocation}
		buckets = append(buckets, bucket)
	}
	resp.Buckets.Buckets = buckets

	return resp
}

func (s *APIService) ListBuckets(request *restful.Request, response *restful.Response) {
	/*if !policy.Authorize(request, response, "bucket:list") {
		return
	}*/
	log.Infof("Received request for all buckets")

	ctx := common.InitCtxWithAuthInfo(request)
	rsp, err := s.s3Client.ListBuckets(ctx, &s3.BaseRequest{})
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
	log.Infoln("List buckets successfully.\n")
}
