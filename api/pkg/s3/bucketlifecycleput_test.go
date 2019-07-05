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
	_ "context"
	"fmt"
	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/client"
	backend "github.com/opensds/multi-cloud/backend/proto/mocks"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	"github.com/opensds/multi-cloud/s3/proto/mocks"
	"golang.org/x/net/context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/micro/go-log"
	. "github.com/opensds/multi-cloud/api/pkg/utils/constants"
	s3 "github.com/opensds/multi-cloud/s3/proto"
)

func TestAPIService_loadStorageClassDefinition(t *testing.T) {
	//mockS3Service:= &S3ServiceMock{}
	//mockS3Service.On("GetStorageClasses").Return(MockedGetStorageClasses())

	s3ClientMock := &mocks.S3Service{}
	backEndMock := &backend.BackendService{}

	s3ClientMock.On("GetStorageClasses", context.Background(), &s3.BaseRequest{}).Return(MockedGetStorageClasses()).
		Once()

	mockApiService := &APIService{
		s3ClientMock,
		backEndMock,
	}
	tests := []struct {
		name    string
		s       *APIService
		wantErr bool
	}{
		{name: "satya", s: mockApiService, wantErr: false},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.s.loadStorageClassDefinition(); (err != nil) != tt.wantErr {
				t.Errorf("APIService.loadStorageClassDefinition() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAPIService_class2tier(t *testing.T) {
	mockS3Service := &S3ServiceMock{}
	mockS3Service.On("GetStorageClasses").Return(MockedGetStorageClasses())
	type args struct {
		name string
	}
	apiService := NewAPIService(client.Client(nil))
	tests := []struct {
		name    string
		s       *APIService
		args    args
		want    int32
		wantErr bool
	}{
		// TODO: Add test cases.
		{name: "satya", s: apiService, args: args{name: "satya"}, want: 0, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.s.class2tier(tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("APIService.class2tier() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("APIService.class2tier() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_checkValidationOfActions(t *testing.T) {
	type args struct {
		actions []*s3.Action
	}
	// Create the type of transition array
	s3ActionArr := make([]*s3.Action, 0)
	//Defining the Transition array and assigning the values tp populate fields
	s3Transition := s3.Action{Name: ActionNameTransition}
	//Assigning the value of days for transition to happen
	s3Transition.Days = 30
	//Assigning the backend value to the s3 struct
	s3Transition.Backend = "AWS"
	s3Transition.Tier = 99

	s3Transition2 := s3.Action{Name: ActionNameTransition}
	//Assigning the value of days for transition to happen
	s3Transition2.Days = 29
	//Assigning the backend value to the s3 struct
	s3Transition2.Backend = "AWS"
	s3Transition2.Tier = 99

	s3Transition3 := s3.Action{Name: ActionNameTransition}
	//Assigning the value of days for transition to happen
	s3Transition3.Days = 45
	//Assigning the backend value to the s3 struct
	s3Transition3.Backend = "AWS"
	s3Transition3.Tier = 999

	s3Transition4 := s3.Action{Name: ActionNameExpiration}
	//Assigning the value of days for transition to happen
	s3Transition4.Days = 95
	//Assigning the backend value to the s3 struct
	s3Transition4.Backend = "AWS"
	s3Transition4.Tier = 999

	s3Transition5 := s3.Action{Name: ActionNameTransition}
	//Assigning the value of days for transition to happen
	s3Transition5.Days = 95
	//Assigning the backend value to the s3 struct
	s3Transition5.Backend = "AWS"
	s3Transition5.Tier = 999

	//Adding the transition value to the main rule
	s3ActionArr = append(s3ActionArr, &s3Transition2)
	s3ActionArr = append(s3ActionArr, &s3Transition)
	s3ActionArr = append(s3ActionArr, &s3Transition3)
	s3ActionArr = append(s3ActionArr, &s3Transition4)
	s3ActionArr = append(s3ActionArr, &s3Transition5)

	argsTest := args{
		actions: s3ActionArr,
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"test", argsTest, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := checkValidationOfActions(tt.args.actions); (err != nil) != tt.wantErr {
				t.Errorf("checkValidationOfActions() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

}

func Test_checkValidationOfActions2(t *testing.T) {
	type args struct {
		actions []*s3.Action
	}
	// Create the type of transition array
	s3ActionArr := make([]*s3.Action, 0)
	//Defining the Transition array and assigning the values tp populate fields
	s3Transition := s3.Action{Name: ActionNameTransition}
	//Assigning the value of days for transition to happen
	s3Transition.Days = 30
	//Assigning the backend value to the s3 struct
	s3Transition.Backend = "AWS"
	s3Transition.Tier = 99

	s3Transition2 := s3.Action{Name: ActionNameTransition}
	//Assigning the value of days for transition to happen
	s3Transition2.Days = 29
	//Assigning the backend value to the s3 struct
	s3Transition2.Backend = "AWS"
	s3Transition2.Tier = 99

	s3Transition3 := s3.Action{Name: ActionNameTransition}
	//Assigning the value of days for transition to happen
	s3Transition3.Days = 45
	//Assigning the backend value to the s3 struct
	s3Transition3.Backend = "AWS"
	s3Transition3.Tier = 999

	s3Transition4 := s3.Action{Name: ActionNameExpiration}
	//Assigning the value of days for transition to happen
	s3Transition4.Days = 0
	//Assigning the backend value to the s3 struct
	s3Transition4.Backend = "AWS"
	s3Transition4.Tier = 999

	s3Transition5 := s3.Action{Name: ActionNameTransition}
	//Assigning the value of days for transition to happen
	s3Transition5.Days = 95
	//Assigning the backend value to the s3 struct
	s3Transition5.Backend = "AWS"
	s3Transition5.Tier = 999

	//Adding the transition value to the main rule
	//s3ActionArr = append(s3ActionArr, &s3Transition2)
	//s3ActionArr = append(s3ActionArr, &s3Transition)
	//s3ActionArr = append(s3ActionArr, &s3Transition3)
	s3ActionArr = append(s3ActionArr, &s3Transition4)
	s3ActionArr = append(s3ActionArr, &s3Transition5)

	argsTest := args{
		actions: s3ActionArr,
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"test", argsTest, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := checkValidationOfActions(tt.args.actions); (err != nil) != tt.wantErr {
				t.Errorf("checkValidationOfActions() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
func Test_checkValidationOfActions3(t *testing.T) {
	type args struct {
		actions []*s3.Action
	}
	// Create the type of transition array
	s3ActionArr := make([]*s3.Action, 0)
	//Defining the Transition array and assigning the values tp populate fields
	s3Transition := s3.Action{Name: ActionNameTransition}
	//Assigning the value of days for transition to happen
	s3Transition.Days = 30
	//Assigning the backend value to the s3 struct
	s3Transition.Backend = "AWS"
	s3Transition.Tier = 99

	s3Transition2 := s3.Action{Name: ActionNameTransition}
	//Assigning the value of days for transition to happen
	s3Transition2.Days = 29
	//Assigning the backend value to the s3 struct
	s3Transition2.Backend = "AWS"
	s3Transition2.Tier = 99

	s3Transition3 := s3.Action{Name: ActionNameTransition}
	//Assigning the value of days for transition to happen
	s3Transition3.Days = 45
	//Assigning the backend value to the s3 struct
	s3Transition3.Backend = "AWS"
	s3Transition3.Tier = 999

	s3Transition4 := s3.Action{Name: ActionNameExpiration}
	//Assigning the value of days for transition to happen
	s3Transition4.Days = 2
	//Assigning the backend value to the s3 struct
	s3Transition4.Backend = "AWS"
	s3Transition4.Tier = 999

	s3Transition5 := s3.Action{Name: ActionNameTransition}
	//Assigning the value of days for transition to happen
	s3Transition5.Days = 5
	//Assigning the backend value to the s3 struct
	s3Transition5.Backend = "AWS"
	s3Transition5.Tier = 999

	//Adding the transition value to the main rule
	//s3ActionArr = append(s3ActionArr, &s3Transition2)
	//s3ActionArr = append(s3ActionArr, &s3Transition)
	//s3ActionArr = append(s3ActionArr, &s3Transition3)
	s3ActionArr = append(s3ActionArr, &s3Transition4)
	s3ActionArr = append(s3ActionArr, &s3Transition5)

	argsTest := args{
		actions: s3ActionArr,
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"test", argsTest, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := checkValidationOfActions(tt.args.actions); (err != nil) != tt.wantErr {
				t.Errorf("checkValidationOfActions() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
func Test_checkValidationOfActions4(t *testing.T) {
	type args struct {
		actions []*s3.Action
	}
	// Create the type of transition array
	s3ActionArr := make([]*s3.Action, 0)
	//Defining the Transition array and assigning the values tp populate fields
	s3Transition := s3.Action{Name: ActionNameTransition}
	//Assigning the value of days for transition to happen
	s3Transition.Days = 20
	//Assigning the backend value to the s3 struct
	s3Transition.Backend = "AWS"
	s3Transition.Tier = 999

	s3Transition2 := s3.Action{Name: ActionNameTransition}
	//Assigning the value of days for transition to happen
	s3Transition2.Days = 29
	//Assigning the backend value to the s3 struct
	s3Transition2.Backend = "AWS"
	s3Transition2.Tier = 99

	s3Transition3 := s3.Action{Name: ActionNameTransition}
	//Assigning the value of days for transition to happen
	s3Transition3.Days = 45
	//Assigning the backend value to the s3 struct
	s3Transition3.Backend = "AWS"
	s3Transition3.Tier = 999

	s3Transition4 := s3.Action{Name: ActionNameExpiration}
	//Assigning the value of days for transition to happen
	s3Transition4.Days = 2
	//Assigning the backend value to the s3 struct
	s3Transition4.Backend = "AWS"
	s3Transition4.Tier = 999

	s3Transition5 := s3.Action{Name: ActionNameTransition}
	//Assigning the value of days for transition to happen
	s3Transition5.Days = 5
	//Assigning the backend value to the s3 struct
	s3Transition5.Backend = "AWS"
	s3Transition5.Tier = 999

	//Adding the transition value to the main rule

	s3ActionArr = append(s3ActionArr, &s3Transition)
	s3ActionArr = append(s3ActionArr, &s3Transition2)
	//s3ActionArr = append(s3ActionArr, &s3Transition3)
	s3ActionArr = append(s3ActionArr, &s3Transition4)
	s3ActionArr = append(s3ActionArr, &s3Transition5)

	argsTest := args{
		actions: s3ActionArr,
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"test", argsTest, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := checkValidationOfActions(tt.args.actions); (err != nil) != tt.wantErr {
				t.Errorf("checkValidationOfActions() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
func Test_checkValidationOfActions5(t *testing.T) {
	type args struct {
		actions []*s3.Action
	}
	// Create the type of transition array
	s3ActionArr := make([]*s3.Action, 0)
	//Defining the Transition array and assigning the values tp populate fields
	s3Transition := s3.Action{Name: ActionNameTransition}
	//Assigning the value of days for transition to happen
	s3Transition.Days = 30
	//Assigning the backend value to the s3 struct
	s3Transition.Backend = "AWS"
	s3Transition.Tier = 99

	s3Transition2 := s3.Action{Name: ActionNameTransition}
	//Assigning the value of days for transition to happen
	s3Transition2.Days = 29
	//Assigning the backend value to the s3 struct
	s3Transition2.Backend = "AWS"
	s3Transition2.Tier = 99

	s3Transition3 := s3.Action{Name: ActionNameTransition}
	//Assigning the value of days for transition to happen
	s3Transition3.Days = 45
	//Assigning the backend value to the s3 struct
	s3Transition3.Backend = "AWS"
	s3Transition3.Tier = 999

	s3Transition4 := s3.Action{Name: ActionNameExpiration}
	//Assigning the value of days for transition to happen
	s3Transition4.Days = 2
	//Assigning the backend value to the s3 struct
	s3Transition4.Backend = "AWS"
	s3Transition4.Tier = 999

	s3Transition5 := s3.Action{Name: ActionNameTransition}
	//Assigning the value of days for transition to happen
	s3Transition5.Days = 5
	//Assigning the backend value to the s3 struct
	s3Transition5.Backend = "AWS"
	s3Transition5.Tier = 999

	//Adding the transition value to the main rule

	s3ActionArr = append(s3ActionArr, &s3Transition)

	s3ActionArr = append(s3ActionArr, &s3Transition4)

	argsTest := args{
		actions: s3ActionArr,
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"test", argsTest, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := checkValidationOfActions(tt.args.actions); (err != nil) != tt.wantErr {
				t.Errorf("checkValidationOfActions() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
func Test_checkValidationOfActions6(t *testing.T) {
	type args struct {
		actions []*s3.Action
	}
	// Create the type of transition array
	s3ActionArr := make([]*s3.Action, 0)

	argsTest := args{
		actions: s3ActionArr,
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"test", argsTest, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := checkValidationOfActions(tt.args.actions); (err != nil) != tt.wantErr {
				t.Errorf("checkValidationOfActions() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

var (
	Result string
	ts     *testing.T
)

func BucketHandler(rq *restful.Request, rp *restful.Response) {

	type dumb struct {
	}
	dmbs := dumb{}
	s3Resp := s3.BaseResponse{"200", "success", dmbs, nil, 0}
	s3ClientMock := &mocks.S3Service{}
	backEndMock := &backend.BackendService{}
	bucket, _ := MockedGetBucket()
	fmt.Printf("test bucket is : %v\n", bucket)
	bkt := &s3.Bucket{Name: "gsnbkt"}
	s3ClientMock.On("UpdateBucket", context.Background(), bucket).Return(&s3Resp, nil).
		Times(5)

	s3ClientMock.On("GetBucket", context.Background(), bkt).Return(bucket, nil).
		Times(5)

	mockApiService := &APIService{
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
		name string
		s    *APIService
		args args
	}{
		{name: "PutbktLifecycletest1", s: mockApiService, args: argsTest},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		ts.Run(tt.name, func(t *testing.T) {

			tt.s.BucketLifecyclePut(tt.args.request, tt.args.response)
		})
	}
	Result = rq.PathParameter("bucketName")
	log.Logf("received request for create bucket lifecycle: %s", Result)
}

func TestAPIService_BucketLifecyclePut(t *testing.T) {
	ts = t
	//========================================================

	// setup service
	ws := new(restful.WebService)
	ws.Consumes(restful.MIME_XML)
	ws.Route(ws.GET("/v1/s3/{bucketName}").To(BucketHandler))
	restful.Add(ws)

	// setup request + writer
	bodyReader := strings.NewReader("<Sample><Value>gsnbkt</Value></Sample>")
	httpRequest, _ := http.NewRequest("GET", "/v1/s3/gsnbkt", bodyReader)
	httpRequest.Header.Set("Content-Type", restful.MIME_XML)
	httpWriter := httptest.NewRecorder()

	// run
	restful.DefaultContainer.ServeHTTP(httpWriter, httpRequest)

	//if Result != "THIS" {
	//	t.Fatalf("Result is actually: %s", Result)
	//}

	//-----------------------------------------------
	//var hreq,_ = http.NewRequest("PUT","/gsnbkt?bucketName=test", nil)

	//var res = restful.NewResponse(httpWriter)
	//var req = restful.NewRequest(httpRequest)
	//
	//s3ClientMock:=  &mocks.S3Service{}
	//backEndMock:=  &backend.BackendService{}
	//
	//bucket,_:=MockedGetBucket()
	//s3ClientMock.On("GetBucket",context.Background(),bucket).Return(MockedGetBucket()).
	//	Once()
	//
	//mockApiService:= &APIService{
	//	s3ClientMock,
	//	backEndMock,
	//}
	//
	//type args struct {
	//	request  *restful.Request
	//	response *restful.Response
	//}
	//
	//argsTest := args{
	//	request:req,
	//	response:res,
	//}
	//
	//tests := []struct {
	//	name string
	//	s    *APIService
	//	args args
	//}{
	//	{name:"satya",s:mockApiService,args:argsTest},
	//	// TODO: Add test cases.
	//}
	//for _, tt := range tests {
	//	t.Run(tt.name, func(t *testing.T) {
	//
	//		tt.s.BucketLifecyclePut(tt.args.request, tt.args.response)
	//	})
	//}
}

func Test_convertRuleFilterToS3Filter(t *testing.T) {

	var filter = model.Filter{"satya"}
	var filter2 = model.Filter{""}

	type args struct {
		filter model.Filter
	}
	s3filtr := s3.LifecycleFilter{Prefix: "satya"}
	//s3filtrEmpty :=	s3.LifecycleFilter{Prefix:""}
	var argTest = args{filter: filter}
	var argTest2 = args{filter: filter2}
	tests := []struct {
		name string
		args args
		want *s3.LifecycleFilter
	}{
		// TODO: Add test cases.
		{name: "incloud", args: argTest, want: &s3filtr},
		{name: "incloud", args: argTest2, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := convertRuleFilterToS3Filter(tt.args.filter); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("convertRuleFilterToS3Filter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_convertRuleUploadToS3Upload(t *testing.T) {

	var upld = model.AbortIncompleteMultipartUpload{12}
	var s3Upld = s3.AbortMultipartUpload{DaysAfterInitiation: 12}
	type args struct {
		upload model.AbortIncompleteMultipartUpload
	}
	var argTest = args{upload: upld}
	tests := []struct {
		name string
		args args
		want *s3.AbortMultipartUpload
	}{
		// TODO: Add test cases.
		{
			name: "obstoobs", args: argTest, want: &s3Upld,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := convertRuleUploadToS3Upload(tt.args.upload); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("convertRuleUploadToS3Upload() = %v, want %v", got, tt.want)
			}
		})
	}
}

//func MockedGetStorageClasses()(*s3.GetStorageClassesResponse, error){
//	//type GetStorageClassesResponse struct {
//	//	Classes              []*StorageClass `protobuf:"bytes,1,rep,name=classes,proto3" json:"classes,omitempty"`
//	//	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
//	//	XXX_unrecognized     []byte          `json:"-"`
//	//	XXX_sizecache        int32           `json:"-"`
//	//}
//	//type StorageClass struct {
//	//	Name                 string   `protobuf:"bytes,1,opt,name=Name,proto3" json:"Name,omitempty"`
//	//	Tier                 int32    `protobuf:"varint,2,opt,name=Tier,proto3" json:"Tier,omitempty"`
//	//	XXX_NoUnkeyedLiteral struct{} `json:"-"`
//	//	XXX_unrecognized     []byte   `json:"-"`
//	//	XXX_sizecache        int32    `json:"-"`
//	//}
//	var stdstorclass = &s3.StorageClass{Name:"STANDARD",Tier:9}
//	var stdiastorclass = &s3.StorageClass{Name:"STANDARD_IA",Tier:99}
//	var storClasArr = []*s3.StorageClass{stdstorclass, stdiastorclass}
//	var resp = s3.GetStorageClassesResponse{Classes:storClasArr}
//	return &resp,nil
//}
