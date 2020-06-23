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
	"github.com/opensds/multi-cloud/s3/proto/mocks"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"
	backend "github.com/opensds/multi-cloud/backend/proto"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	s3 "github.com/opensds/multi-cloud/s3/proto"
	"golang.org/x/net/context"

	backendMock "github.com/opensds/multi-cloud/backend/proto/mocks"
)

func TestAPIService_tier2class(t *testing.T) {

	s3ClientMock := &mocks.S3Service{}
	s3ClientMock.On("GetStorageClasses", context.Background(), &s3.BaseRequest{}).Return(MockedGetStorageClasses()).
		Once()

	//mockS3Service := &S3ServiceMock{}
	//mockS3Service.On("GetStorageClasses").Return(MockedGetStorageClasses())
	backEndMock := &backendMock.BackendService{}

	type fields struct {
		s3Client      s3.S3Service
		backendClient backend.BackendService
	}
	fields1:= fields{
		s3ClientMock,
		backEndMock,
	}
	type args struct {
		tier int32
	}
	args2:=args{99}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		{"test1",fields1,args2,"STANDARD_IA",false},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &APIService{
				s3Client:      tt.fields.s3Client,
				backendClient: tt.fields.backendClient,
			}
			got, err := s.tier2class(tt.args.tier)
			if (err != nil) != tt.wantErr {
				t.Errorf("APIService.tier2class() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("APIService.tier2class() = %v, want %v", got, tt.want)
			}
		})
	}
}


func BucketGetHandler(rq *restful.Request, rp *restful.Response) {

	type dumb struct {
	}
	dmbs := dumb{}
	s3Resp := s3.BaseResponse{"200", "success", dmbs, nil, 0}
	s3ClientMock := &mocks.S3Service{}
	backEndMock := &backendMock.BackendService{}
	s3ClientMock.On("GetStorageClasses", context.Background(), &s3.BaseRequest{}).Return(MockedGetStorageClasses()).
		Once()

	bucket, _ := MockedGetBucket()
	fmt.Printf("test bucket is : %v\n", bucket)
	bkt := &s3.Bucket{Name: "gsnbkt"}
	s3ClientMock.On("UpdateBucket", context.Background(), bucket).Return(&s3Resp, nil).
		Times(5)

	s3ClientMock.On("GetBucket", context.Background(), bkt).Return(bucket, nil).
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
		{"getbucketTest1",fields1,argsTest},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		ts.Run(tt.name, func(t *testing.T) {
			s := &APIService{
				s3Client:      tt.fields.s3Client,
				backendClient: tt.fields.backendClient,
			}
			s.BucketLifecycleGet(tt.args.request, tt.args.response)
		})
	}
	Result = rq.PathParameter("bucketName")
	log.Logf("received request for create bucket lifecycle: %s", Result)
}

func TestAPIService_BucketLifecycleGet(t *testing.T) {

	ts = t
	//========================================================

	// setup service
	ws := new(restful.WebService)
	ws.Consumes(restful.MIME_XML)
	ws.Route(ws.GET("/v1/s3/{bucketName}").To(BucketGetHandler))
	restful.Add(ws)

	// setup request + writer
	bodyReader := strings.NewReader("<Sample><Value>gsnbkt</Value></Sample>")
	httpRequest, _ := http.NewRequest("GET", "/v1/s3/gsnbkt", bodyReader)
	httpRequest.Header.Set("Content-Type", restful.MIME_XML)
	httpWriter := httptest.NewRecorder()

	// run
	restful.DefaultContainer.ServeHTTP(httpWriter, httpRequest)


}

func Test_converts3FilterToRuleFilter(t *testing.T) {

	var filter = model.Filter{"satya"}
	var filter2 = model.Filter{"gsn"}


	s3filtr := s3.LifecycleFilter{Prefix: "satya"}
	s3filtr2 := s3.LifecycleFilter{Prefix: "gsn"}
	//s3filtrEmpty :=	s3.LifecycleFilter{Prefix:""}


	type args struct {
		filter *s3.LifecycleFilter
	}
	var argTest = args{filter: &s3filtr}
	var argTest2 = args{filter: &s3filtr2}
	tests := []struct {
		name string
		args args
		want model.Filter
	}{
		{name: "incloud", args: argTest, want: filter},
		{name: "incloud", args: argTest2, want: filter2},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := converts3FilterToRuleFilter(tt.args.filter); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("converts3FilterToRuleFilter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_converts3UploadToRuleUpload(t *testing.T) {


	var upld = model.AbortIncompleteMultipartUpload{12}
	var s3Upld = s3.AbortMultipartUpload{DaysAfterInitiation: 12}
	type args struct {
		upload *s3.AbortMultipartUpload
	}
	var argTest = args{upload: &s3Upld}
	tests := []struct {
		name string
		args args
		want model.AbortIncompleteMultipartUpload
	}{
		{name: "obstoobs", args: argTest, want: upld,},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := converts3UploadToRuleUpload(tt.args.upload); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("converts3UploadToRuleUpload() = %v, want %v", got, tt.want)
			}
		})
	}


}
