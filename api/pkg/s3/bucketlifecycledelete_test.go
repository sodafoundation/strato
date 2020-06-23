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
	"fmt"
	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"
	backend "github.com/opensds/multi-cloud/backend/proto"
	backendMock "github.com/opensds/multi-cloud/backend/proto/mocks"
	s3 "github.com/opensds/multi-cloud/s3/proto"
	"github.com/opensds/multi-cloud/s3/proto/mocks"
	"golang.org/x/net/context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)



var (
	Result1 string
	tx     *testing.T
)

func BucketDelHandler(rq *restful.Request, rp *restful.Response) {

	type dumb struct {
	}
	dmbs := dumb{}
	s3Resp := s3.BaseResponse{"200", "success", dmbs, nil, 0}
	s3ClientMock := &mocks.S3Service{}
	backEndMock := &backendMock.BackendService{}
	s3ClientMock.On("GetStorageClasses", context.Background(), &s3.BaseRequest{}).Return(MockedGetStorageClasses()).
		Once()
	bkt := &s3.Bucket{Name: "gsnbkt"}
	bucket, _ := MockedGetBucket()
	s3ClientMock.On("GetBucket", context.Background(), bkt).Return(bucket, nil).
		Times(5)

	delBktCycle:= s3.DeleteLifecycleInput{}
	delBktCycle.Bucket="gsnbkt"
	delBktCycle.RuleID="rid"


	fmt.Printf("test bucket is : %v\n", bucket)

	s3ClientMock.On("DeleteBucketLifecycle", context.Background(), &delBktCycle).Return(&s3Resp, nil).
		Times(5)



	type fields struct {
		s3Client      s3.S3Service
		backendClient backend.BackendService
	}
	fields1:= fields{
		s3ClientMock,
		backEndMock,
	}
	type args struct {
		request  *restful.Request
		response *restful.Response
	}
	argsTest := args{
		request:  rq,
		response: rp,
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"delbucketTest1",fields1,argsTest},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		tx.Run(tt.name, func(t *testing.T) {
			s := &APIService{
				s3Client:      tt.fields.s3Client,
				backendClient: tt.fields.backendClient,
			}
			s.BucketLifecycleDelete(tt.args.request, tt.args.response)
		})
	}
	Result = rq.PathParameter("bucketName")
	log.Logf("received request for delete bucket lifecycle: %s", Result1)
}
func TestAPIService_BucketLifecycleDelete(t *testing.T) {

	tx = t
	//========================================================

	// setup service
	ws := new(restful.WebService)
	ws.Consumes(restful.MIME_XML)
	ws.Route(ws.DELETE("/v1/s3/{bucketName}").To(BucketDelHandler))
	restful.Add(ws)

	// setup request + writer
	bodyReader := strings.NewReader("<Sample><Value>gsnbkt</Value></Sample>")
	httpRequest, _ := http.NewRequest("DELETE", "/v1/s3/gsnbkt/?lifecycle&ruleID=rid", bodyReader)
	httpRequest.Header.Set("Content-Type", restful.MIME_XML)
	httpWriter := httptest.NewRecorder()

	// run
	restful.DefaultContainer.ServeHTTP(httpWriter, httpRequest)
}
