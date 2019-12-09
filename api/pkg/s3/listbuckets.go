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
	"github.com/opensds/multi-cloud/s3/pkg/utils"
	"time"

	"github.com/emicklei/go-restful"
	"github.com/opensds/multi-cloud/api/pkg/common"
	"github.com/opensds/multi-cloud/api/pkg/policy"
	. "github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	"github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

func parseListBuckets(list *s3.ListBucketsResponse) []byte {
	if list == nil || list.Buckets == nil {
		return nil
	}
	temp := model.ListAllMyBucketsResult{}

	log.Infof("Parse ListBuckets: %v", list.Buckets)
	//default xmlns
	temp.Xmlns = model.Xmlns
	buckets := []model.Bucket{}
	for _, value := range list.Buckets {
		ctime := time.Unix(value.CreateTime, 0).Format(time.RFC3339)
		versionOpts := model.VersioningConfiguration{}
		if value.Versioning != nil {
			if value.Versioning.Status == utils.VersioningEnabled {
				versionOpts.Status = utils.VersioningEnabled
			}
		}
		sseOpts := model.SSEConfiguration{}
		if value.ServerSideEncryption != nil {
			if value.ServerSideEncryption.SseType == "SSE" {
				sseOpts.SSE.Enabled = "true"
			}
		}
		bucket := model.Bucket{Name: value.Name, CreateTime: ctime, LocationConstraint: value.DefaultLocation, SSEOpts: sseOpts}
		buckets = append(buckets, bucket)
	}
	temp.Buckets = buckets

	xmlstring, err := xml.MarshalIndent(temp, "", "  ")
	if err != nil {
		log.Errorf("Parse ListBuckets error: %v", err)
		return nil
	}
	xmlstring = []byte(xml.Header + string(xmlstring))
	return xmlstring
}

func (s *APIService) ListBuckets(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "bucket:list") {
		return
	}
	log.Infof("Received request for all buckets")

	ctx := common.InitCtxWithAuthInfo(request)
	rsp, err := s.s3Client.ListBuckets(ctx, &s3.BaseRequest{})
	errCode := int32(0)
	if rsp != nil && rsp.ErrorCode != int32(ErrNoErr){
		errCode = rsp.ErrorCode
	}
	if HandleS3Error(response, request, err, errCode) != nil {
		log.Errorf("list bucket failed, err=%v, errCode=%d\n", err, errCode)
		return
	}

	realRes := parseListBuckets(rsp)

	log.Infof("Get List of buckets successfully:%v\n", string(realRes))
	response.Write(realRes)
}
