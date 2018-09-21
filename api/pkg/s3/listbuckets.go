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
	"github.com/micro/go-log"
	"net/http"
	//	"github.com/micro/go-micro/errors"
	"encoding/xml"
	"github.com/opensds/multi-cloud/s3/proto"
	"golang.org/x/net/context"
	"strings"
	"time"
)

func parseListBuckets(list *s3.ListBucketsResponse) *string {
	result := []string{}
	if list == nil || list.Buckets == nil {
		return nil
	}
	temp := ListAllMyBucketsResult{}

	log.Logf("Parse ListBuckets: %v", list.Buckets)
	//default xmlns
	temp.Xmlns = Xmlns
	buckets := []Bucket{}
	for _, value := range list.Buckets {
		creationDate := time.Unix(value.CreationDate, 0).Format(time.RFC3339)
		bucket := Bucket{Name: value.Name, CreationDate: creationDate}
		buckets = append(buckets, bucket)
	}
	temp.Buckets = buckets

	data, err := xml.MarshalIndent(temp, "", "")
	if err != nil {
		log.Logf("Parse ListBuckets error: %v", err)
		return nil
	}
	result = append(result, xml.Header)
	result = append(result, string(data))
	str := strings.Join(result, "")
	return &str
}

func (s *APIService) ListBuckets(request *restful.Request, response *restful.Response) {
	ctx := context.Background()
	//TODO owner
	owner := "test"
	log.Logf("Received request for all buckets")
	res, err := s.s3Client.ListBuckets(ctx, &s3.BaseRequest{Id: owner})
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	realRes := parseListBuckets(res)

	log.Logf("Get List of buckets successfully:%v\n", realRes)

	response.WriteEntity(realRes)

}
