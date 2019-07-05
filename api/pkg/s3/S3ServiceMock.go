package s3

import (
	"fmt"
	"github.com/micro/go-micro/client"
	. "github.com/opensds/multi-cloud/api/pkg/utils/constants"
	s3 "github.com/opensds/multi-cloud/s3/proto"
	"github.com/stretchr/testify/mock"
)
import (
	"context"
)

type S3ServiceMock struct {
	mock.Mock
}

func GetStorageClasses(ctx context.Context, in *s3.BaseRequest, opts ...client.CallOption) (*s3.GetStorageClassesResponse, error) {
	return MockedGetStorageClasses()
}
func MockedGetStorageClasses() (*s3.GetStorageClassesResponse, error) {
	//type GetStorageClassesResponse struct {
	//	Classes              []*StorageClass `protobuf:"bytes,1,rep,name=classes,proto3" json:"classes,omitempty"`
	//	XXX_NoUnkeyedLiteral struct{}        `json:"-"`
	//	XXX_unrecognized     []byte          `json:"-"`
	//	XXX_sizecache        int32           `json:"-"`
	//}
	//type StorageClass struct {
	//	Name                 string   `protobuf:"bytes,1,opt,name=Name,proto3" json:"Name,omitempty"`
	//	Tier                 int32    `protobuf:"varint,2,opt,name=Tier,proto3" json:"Tier,omitempty"`
	//	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	//	XXX_unrecognized     []byte   `json:"-"`
	//	XXX_sizecache        int32    `json:"-"`
	//}
	var stdstorclass = &s3.StorageClass{Name: "STANDARD", Tier: 9}
	var stdiastorclass = &s3.StorageClass{Name: "STANDARD_IA", Tier: 99}
	var storClasArr = []*s3.StorageClass{stdstorclass, stdiastorclass}
	var resp = s3.GetStorageClassesResponse{Classes: storClasArr}
	return &resp, nil
}

func create_lifecycle_json_fake_dat() s3.Bucket {

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
	var LifecycleRule1 = s3.LifecycleRule{}
	LifecycleRule1.Id = "rid"
	LifecycleRule1.Filter = nil
	LifecycleRule1.Status = "Enabled"
	LifecycleRule1.Actions = s3ActionArr
	LifecycleRule1.AbortIncompleteMultipartUpload = nil

	bucket := s3.Bucket{Name: "gsnbkt"}
	bucket.LifecycleConfiguration = []*s3.LifecycleRule{&LifecycleRule1}
	return bucket
}
func MockedGetBucket() (*s3.Bucket, error) {
	bucket1 := create_lifecycle_json_fake_dat()
	fmt.Printf("bcukemt is:%v\n", bucket1)
	return &bucket1, nil
}
