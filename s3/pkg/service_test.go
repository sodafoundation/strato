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

package pkg

import (
	"context"
	"github.com/opensds/multi-cloud/s3/pkg/db"
	"github.com/opensds/multi-cloud/s3/pkg/db/mocks"
	"net/http"
	"os"
	"reflect"
	"testing"

	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	pb "github.com/opensds/multi-cloud/s3/proto"

	. "github.com/opensds/multi-cloud/s3/pkg/utils"
	data "github.com/opensds/multi-cloud/testutils/collection"
)

func Test_getNameFromTier(t *testing.T) {

	t2n := make(Int2String)
	t2n[Tier1] = AWS_STANDARD
	t2n[Tier99] = AWS_STANDARD_IA
	t2n[Tier999] = AWS_GLACIER

	Int2ExtTierMap = make(map[string]*Int2String)
	(Int2ExtTierMap)[OSTYPE_OPENSDS] = &t2n

	type args struct {
		tier int32
	}
	argsTest:= args{1}
	argsTest9:= args{9}
	argsTest99:= args{99}
	argsTest999:= args{999}

	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		// TODO: Add test cases.
		{"testNameFromTier1",argsTest,"STANDARD",false},
		{"testNameFromTier2",argsTest9,"",true},
		{"testNameFromTier3",argsTest99,AWS_STANDARD_IA,false},
		{"testNameFromTier4",argsTest999,AWS_GLACIER,false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getNameFromTier(tt.args.tier)
			if (err != nil) != tt.wantErr {
				t.Errorf("getNameFromTier() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("getNameFromTier() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_loadAWSDefault(t *testing.T) {
	type args struct {
		i2e *map[string]*Int2String
		e2i *map[string]*String2Int
	}
	i2e:= make(map[string]*Int2String)
	e2i:=make(map[string]*String2Int)
	argsTest:= args{&i2e,&e2i}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
		{"loadAwsTest1",argsTest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loadAWSDefault(tt.args.i2e, tt.args.e2i)
			data.AssertTestResult(t, len(i2e), 1)
			data.AssertTestResult(t, len(*i2e[OSTYPE_AWS]), 3)
			data.AssertTestResult(t, len(e2i), 1)
			data.AssertTestResult(t, len(*e2i[OSTYPE_AWS]), 3)
		})
	}
}

func Test_loadOpenSDSDefault(t *testing.T) {
	type args struct {
		i2e *map[string]*Int2String
		e2i *map[string]*String2Int
	}
	i2e:= make(map[string]*Int2String)
	e2i:=make(map[string]*String2Int)
	argsTest:= args{&i2e,&e2i}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
		{"loadOpenSDSTest1",argsTest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loadOpenSDSDefault(tt.args.i2e, tt.args.e2i)
			data.AssertTestResult(t, len(i2e), 1)
			data.AssertTestResult(t, len(*i2e[OSTYPE_OPENSDS]), 3)
			data.AssertTestResult(t, len(e2i), 1)
			data.AssertTestResult(t, len(*e2i[OSTYPE_OPENSDS]), 3)
		})
	}
}

func Test_loadAzureDefault(t *testing.T) {
	type args struct {
		i2e *map[string]*Int2String
		e2i *map[string]*String2Int
	}
	i2e:= make(map[string]*Int2String)
	e2i:=make(map[string]*String2Int)
	argsTest:= args{&i2e,&e2i}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
		{"loadAzureTest1",argsTest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loadAzureDefault(tt.args.i2e, tt.args.e2i)
			data.AssertTestResult(t, len(i2e), 1)
			data.AssertTestResult(t, len(*i2e[OSTYPE_Azure]), 3)
			data.AssertTestResult(t, len(e2i), 1)
			data.AssertTestResult(t, len(*e2i[OSTYPE_Azure]), 3)
		})
	}
}

func Test_loadHWDefault(t *testing.T) {
	type args struct {
		i2e *map[string]*Int2String
		e2i *map[string]*String2Int
	}
	i2e:= make(map[string]*Int2String)
	e2i:=make(map[string]*String2Int)
	argsTest:= args{&i2e,&e2i}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
		{"loadHwsTest1",argsTest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loadHWDefault(tt.args.i2e, tt.args.e2i)
			data.AssertTestResult(t, len(i2e), 1)
			data.AssertTestResult(t, len(*i2e[OSTYPE_OBS]), 3)
			data.AssertTestResult(t, len(e2i), 1)
			data.AssertTestResult(t, len(*e2i[OSTYPE_OBS]), 3)
		})
	}
}

func Test_loadGCPDefault(t *testing.T) {
	type args struct {
		i2e *map[string]*Int2String
		e2i *map[string]*String2Int
	}
	i2e:= make(map[string]*Int2String)
	e2i:=make(map[string]*String2Int)
	argsTest:= args{&i2e,&e2i}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
		{"loadHwsTest1",argsTest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loadGCPDefault(tt.args.i2e, tt.args.e2i)
			data.AssertTestResult(t, len(i2e), 1)
			data.AssertTestResult(t, len(*i2e[OSTYPE_GCS]), 1)
			data.AssertTestResult(t, len(e2i), 1)


		})

	}
}

func Test_loadCephDefault(t *testing.T) {
	type args struct {
		i2e *map[string]*Int2String
		e2i *map[string]*String2Int
	}
	i2e:= make(map[string]*Int2String)
	e2i:=make(map[string]*String2Int)
	argsTest:= args{&i2e,&e2i}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
		{"loadcephTest1",argsTest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loadCephDefault(tt.args.i2e, tt.args.e2i)
			data.AssertTestResult(t, len(i2e), 1)
			data.AssertTestResult(t, len(*i2e[OSTYPE_CEPTH]), 1)
			data.AssertTestResult(t, len(e2i), 1)
			data.AssertTestResult(t, len(*e2i[OSTYPE_CEPTH]), 1)
		})
	}
}

func Test_loadFusionStroageDefault(t *testing.T) {
	type args struct {
		i2e *map[string]*Int2String
		e2i *map[string]*String2Int
	}
	i2e:= make(map[string]*Int2String)
	e2i:=make(map[string]*String2Int)
	argsTest:= args{&i2e,&e2i}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
		{"loadFusionTest1",argsTest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loadFusionStroageDefault(tt.args.i2e, tt.args.e2i)
			data.AssertTestResult(t, len(i2e), 1)
			data.AssertTestResult(t, len(*i2e[OSTYPE_FUSIONSTORAGE]), 1)
			data.AssertTestResult(t, len(e2i), 1)
			data.AssertTestResult(t, len(*e2i[OSTYPE_FUSIONSTORAGE]), 1)

		})
	}
}

func Test_loadDefaultStorageClass(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{"loadDefaultTest1",false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := loadDefaultStorageClass(); (err != nil) != tt.wantErr {

				t.Errorf("loadDefaultStorageClass() error = %v, wantErr %v", err, tt.wantErr)
			}

			i2e:=Int2ExtTierMap
			e2i:=Ext2IntTierMap
			data.AssertTestResult(t, len(i2e), 7)
			data.AssertTestResult(t, len(*i2e[OSTYPE_FUSIONSTORAGE]), 1)
			data.AssertTestResult(t, len(e2i), 7)
			data.AssertTestResult(t, len(*e2i[OSTYPE_FUSIONSTORAGE]), 1)
			data.AssertTestResult(t, len(*i2e[OSTYPE_GCS]), 1)
			data.AssertTestResult(t, len(*e2i[OSTYPE_GCS]), 1)
			data.AssertTestResult(t, len(*i2e[OSTYPE_OBS]), 3)
			data.AssertTestResult(t, len(*e2i[OSTYPE_OBS]), 3)
			data.AssertTestResult(t, len(*i2e[OSTYPE_AWS]), 3)
			data.AssertTestResult(t, len(*e2i[OSTYPE_AWS]), 3)
			data.AssertTestResult(t, len(*i2e[OSTYPE_Azure]), 3)
			data.AssertTestResult(t, len(*e2i[OSTYPE_Azure]), 3)
			data.AssertTestResult(t, len(*i2e[OSTYPE_OPENSDS]), 3)
			data.AssertTestResult(t, len(*e2i[OSTYPE_OPENSDS]), 3)
			data.AssertTestResult(t, len(*i2e[OSTYPE_CEPTH]), 1)
			data.AssertTestResult(t, len(*e2i[OSTYPE_CEPTH]), 1)
		})
	}
}

func Test_loadUserDefinedStorageClass(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		// TODO: Add test cases.
		{"testUsrDefStorclass",true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := loadUserDefinedStorageClass(); (err != nil) != tt.wantErr {
				t.Errorf("loadUserDefinedStorageClass() error = %v, wantErr %v", err, tt.wantErr)
			}


		})
	}
}

func Test_loadDefaultTransition(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		// TODO: Add test cases.
		{"testLoadDefTrans",false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := loadDefaultTransition(); (err != nil) != tt.wantErr {
				t.Errorf("loadDefaultTransition() error = %v, wantErr %v", err, tt.wantErr)
			}
			data.AssertTestResult(t, len(TransitionMap), 3)
			data.AssertTestResult(t, TransitionMap[Tier999], []int32{Tier1, Tier99, Tier999})
		})
	}
}

func Test_loadUserDefinedTransition(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		// TODO: Add test cases.
		{"testUserDefinedTrans",true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := loadUserDefinedTransition(); (err != nil) != tt.wantErr {
				t.Errorf("loadUserDefinedTransition() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

//func Test_initStorageClass(t *testing.T) {
//	tests := []struct {
//		name string
//	}{
//		// TODO: Add test cases.
//		{"testInitStorage"},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			initStorageClass()
//		})
//	}
//}
func Test_initStorageClass2(t *testing.T) {
	tests := []struct {
		name string
	}{
		// TODO: Add test cases.
		{"testInitStorage with AWS"},
	}
	os.Setenv("USE_DEFAULT_STORAGE_CLASS","1")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			initStorageClass()
		})
	}
}
//func Test_initStorageClass3(t *testing.T) {
//	tests := []struct {
//		name string
//	}{
//		// TODO: Add test cases.
//		{"testInitStorage with empty space"},
//	}
//	os.Setenv("USE_DEFAULT_STORAGE_CLASS","  ")
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			initStorageClass()
//		})
//	}
//}
func Test_initStorageClass4(t *testing.T) {
	tests := []struct {
		name string
	}{
		// TODO: Add test cases.
		{"testInitStorage with zero"},
	}
	os.Setenv("USE_DEFAULT_STORAGE_CLASS","0")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			initStorageClass()
		})
	}
}
//func Test_initStorageClass5(t *testing.T) {
//	tests := []struct {
//		name string
//	}{
//		// TODO: Add test cases.
//		{"testInitStorage with large number"},
//	}
//	os.Setenv("USE_DEFAULT_STORAGE_CLASS","999999999999999999999999999999999999999999999999999999")
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			initStorageClass()
//		})
//	}
//}
func Test_s3Service_GetStorageClasses(t *testing.T) {
	type args struct {
		ctx context.Context
		in  *pb.BaseRequest
		out *pb.GetStorageClassesResponse
	}
	argsTemp:=args{}
	argsTemp.ctx=context.Background()
	argsTemp.in=&pb.BaseRequest{}
	argsTemp.out = &pb.GetStorageClassesResponse{}

	SupportedClasses = append(SupportedClasses, pb.StorageClass{Name: string(AWS_STANDARD), Tier: int32(Tier1)})
	SupportedClasses = append(SupportedClasses, pb.StorageClass{Name: string(AWS_STANDARD_IA), Tier: int32(Tier99)})
	SupportedClasses = append(SupportedClasses, pb.StorageClass{Name: string(AWS_GLACIER), Tier: int32(Tier999)})

	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"TestGetStorageClss",&s3Service{},argsTemp,false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.GetStorageClasses(tt.args.ctx, tt.args.in, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.GetStorageClasses() error = %v, wantErr %v", err, tt.wantErr)
			}
			data.AssertTestResult(t, len(tt.args.out.Classes), 3)
		})
	}
}

func Test_s3Service_ListBuckets(t *testing.T) {

	listBucktResp:=data.GetBuckets()
	in:= pb.BaseRequest{}
	out:= []pb.Bucket{}
	//buckets:=listBucktResp.Buckets

	//for j := 0; j < len(buckets); j++ {
	//	out = append(out, *buckets[j])
	//}
	dbMock := new(mocks.DBAdapter)
	db.DbAdapter = dbMock

	err:=S3Error{}
	err.Code=ERR_OK

	dbMock.On("ListBuckets", &in, &out).Return(err).
		Once()



	type args struct {
		ctx context.Context
		in  *pb.BaseRequest
		out *pb.ListBucketsResponse
	}
	argTest:=args{context.Background(),&in,&listBucktResp}
	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"testListBuckets",&s3Service{},argTest,false},
	}
	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.ListBuckets(tt.args.ctx, tt.args.in, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.ListBuckets() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_s3Service_CreateBucket(t *testing.T) {
	type args struct {
		ctx context.Context
		in  *pb.Bucket
		out *pb.BaseResponse
	}

	dbMock := new(mocks.DBAdapter)
	db.DbAdapter = dbMock

	err:=S3Error{}
	err.Code=ERR_OK
	outbkt:=pb.Bucket{}
	in:= pb.Bucket{}
	in.Name="gsnbkt1"
	in.Deleted=false

	dbMock.On("GetBucketByName", "gsnbkt1", &outbkt).Return(err).
		Once()

	dbMock.On("UpdateBucket", &in).Return(err).
		Once()

	type dumb struct {
	}
	dmbs := dumb{}
	s3Resp := pb.BaseResponse{"200", "success", dmbs, nil, 0}
	b := &s3Service{}

	argsTmp:=args{context.Background(),&in,&s3Resp}
	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"createBucketTest",b,argsTmp,false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.CreateBucket(tt.args.ctx, tt.args.in, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.CreateBucket() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_s3Service_CreateBucket2(t *testing.T) {
	type args struct {
		ctx context.Context
		in  *pb.Bucket
		out *pb.BaseResponse
	}

	dbMock := new(mocks.DBAdapter)
	db.DbAdapter = dbMock

	err:=S3Error{}
	err.Code=ERR_OK
	outbkt:=pb.Bucket{}
	in:= pb.Bucket{}
	in.Name="gsnbkt1"
	in.Deleted=false

	errforGetBkt:=S3Error{}
	errforGetBkt.Code=http.StatusNotFound

	errforCreateBkt:=S3Error{}
	errforCreateBkt.Code=ERR_OK


	dbMock.On("GetBucketByName", "gsnbkt1", &outbkt).Return(errforGetBkt).
		Once()

	dbMock.On("UpdateBucket", &in).Return(err).
		Once()

	dbMock.On("CreateBucket", &in).Return(errforCreateBkt).
		Once()

	type dumb struct {
	}
	dmbs := dumb{}
	s3Resp := pb.BaseResponse{"200", "success", dmbs, nil, 0}
	b := &s3Service{}

	argsTmp:=args{context.Background(),&in,&s3Resp}
	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"createBucketTest",b,argsTmp,false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.CreateBucket(tt.args.ctx, tt.args.in, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.CreateBucket() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
func Test_s3Service_CreateBucket3(t *testing.T) {
	type args struct {
		ctx context.Context
		in  *pb.Bucket
		out *pb.BaseResponse
	}

	dbMock := new(mocks.DBAdapter)
	db.DbAdapter = dbMock

	err:=S3Error{}
	err.Code=ERR_OK
	outbkt:=pb.Bucket{}
	in:= pb.Bucket{}
	in.Name="gsnbkt1"
	in.Deleted=false

	errforGetBkt:=S3Error{}
	errforGetBkt.Code=http.StatusNotFound

	errforCreateBkt:=S3Error{}
	errforCreateBkt.Code=201


	dbMock.On("GetBucketByName", "gsnbkt1", &outbkt).Return(errforGetBkt).
		Once()

	dbMock.On("UpdateBucket", &in).Return(err).
		Once()

	dbMock.On("CreateBucket", &in).Return(errforCreateBkt).
		Once()

	type dumb struct {
	}
	dmbs := dumb{}
	s3Resp := pb.BaseResponse{"200", "success", dmbs, nil, 0}
	b := &s3Service{}

	argsTmp:=args{context.Background(),&in,&s3Resp}
	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"createBucketTest",b,argsTmp,true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.CreateBucket(tt.args.ctx, tt.args.in, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.CreateBucket() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}



func Test_s3Service_CreateBucket4(t *testing.T) {
	type args struct {
		ctx context.Context
		in  *pb.Bucket
		out *pb.BaseResponse
	}

	dbMock := new(mocks.DBAdapter)
	db.DbAdapter = dbMock

	err:=S3Error{}
	err.Code=201
	outbkt:=pb.Bucket{}
	in:= pb.Bucket{}
	in.Name="gsnbkt1"
	in.Deleted=false

	errforGetBkt:=S3Error{}
	errforGetBkt.Code=ERR_OK

	errforCreateBkt:=S3Error{}
	errforCreateBkt.Code=201


	dbMock.On("GetBucketByName", "gsnbkt1", &outbkt).Return(errforGetBkt).
		Once()

	dbMock.On("UpdateBucket", &in).Return(err).
		Once()

	dbMock.On("CreateBucket", &in).Return(errforCreateBkt).
		Once()

	type dumb struct {
	}
	dmbs := dumb{}
	s3Resp := pb.BaseResponse{"200", "success", dmbs, nil, 0}
	b := &s3Service{}

	argsTmp:=args{context.Background(),&in,&s3Resp}
	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"createBucketTest",b,argsTmp,true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.CreateBucket(tt.args.ctx, tt.args.in, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.CreateBucket() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_s3Service_GetBucket(t *testing.T) {
	type args struct {
		ctx context.Context
		in  *pb.Bucket
		out *pb.Bucket
	}

	err:=S3Error{}
	err.Code=200
	outbkt:=pb.Bucket{}
	in:= pb.Bucket{}
	in.Name="gsnbkt1"
	in.Deleted=false
	b := &s3Service{}
	argsTmp:=args{context.Background(),&in,&outbkt}
	dbMock := new(mocks.DBAdapter)
	db.DbAdapter = dbMock

	dbMock.On("GetBucketByName", "gsnbkt1", &outbkt).Return(err).
		Once()

	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"getBucketTest1",b,argsTmp,false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.GetBucket(tt.args.ctx, tt.args.in, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.GetBucket() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
func Test_s3Service_GetBucket2(t *testing.T) {
	type args struct {
		ctx context.Context
		in  *pb.Bucket
		out *pb.Bucket
	}

	err:=S3Error{}
	err.Code=201
	outbkt:=pb.Bucket{}
	in:= pb.Bucket{}
	in.Name="gsnbkt1"
	in.Deleted=false
	b := &s3Service{}
	argsTmp:=args{context.Background(),&in,&outbkt}
	dbMock := new(mocks.DBAdapter)
	db.DbAdapter = dbMock

	dbMock.On("GetBucketByName", "gsnbkt1", &outbkt).Return(err).
		Once()

	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"getBucketTest2",b,argsTmp,false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.GetBucket(tt.args.ctx, tt.args.in, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.GetBucket() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
func Test_s3Service_DeleteBucket(t *testing.T) {
	type args struct {
		ctx context.Context
		in  *pb.Bucket
		out *pb.BaseResponse
	}

	dbMock := new(mocks.DBAdapter)
	db.DbAdapter = dbMock

	err:=S3Error{}
	err.Code=200
	outbkt:=pb.Bucket{}
	in:= pb.Bucket{}
	in.Name="gsnbkt1"
	in.Deleted=false

	errforGetBkt:=S3Error{}
	errforGetBkt.Code=ERR_OK

	errforCreateBkt:=S3Error{}
	errforCreateBkt.Code=201

	updatebkt:=pb.Bucket{}
	updatebkt.Deleted=true
	dbMock.On("GetBucketByName", "gsnbkt1", &outbkt).Return(errforGetBkt).
		Once()

	dbMock.On("UpdateBucket", &updatebkt).Return(err).
		Once()

	dbMock.On("CreateBucket", &in).Return(errforCreateBkt).
		Once()

	type dumb struct {
	}
	dmbs := dumb{}
	s3Resp := pb.BaseResponse{"200", "success", dmbs, nil, 0}
	b := &s3Service{}
	argsTmp:=args{context.Background(),&in,&s3Resp}
	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"deleteBucketTest",b, argsTmp,false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.DeleteBucket(tt.args.ctx, tt.args.in, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.DeleteBucket() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_s3Service_ListObjects(t *testing.T) {
	type args struct {
		ctx context.Context
		in  *pb.ListObjectsRequest
		out *pb.ListObjectResponse
	}

	dbMock := new(mocks.DBAdapter)
	db.DbAdapter = dbMock

	err:=S3Error{}
	err.Code=ERR_OK



	objReq:=pb.ListObjectsRequest{}
	objReq.Bucket="gnsbkt1"

	objResp:=pb.ListObjectResponse{}
	objResp.ListObjects=[]*pb.Object{}//data.GetObjectsList("gsnbkt1")
	objArr:=[]pb.Object{}//data.GetObjectsList("gsnbkt1")
	b := &s3Service{}
	argsTmp:=args{context.Background(),&objReq,&objResp}

	dbMock.On("ListObjects", &objReq, &objArr).Return(err).
		Once()
	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"listObjTest1",b,argsTmp,false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.ListObjects(tt.args.ctx, tt.args.in, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.ListObjects() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_s3Service_CreateObject(t *testing.T) {
	type args struct {
		ctx context.Context
		in  *pb.Object
		out *pb.BaseResponse
	}

	dbMock := new(mocks.DBAdapter)
	db.DbAdapter = dbMock

	err:=S3Error{}
	err.Code=200
	outobj:=pb.Object{}
	//outobj.BucketName="gsnbkt1"
	in:= pb.Bucket{}
	in.Name="gsnbkt1"
	in.Deleted=false
	objInput:=pb.GetObjectInput{}
	//objInput.Bucket="gsnbkt1"
	//objInput.Key="gsnbkt_obj"
	b := &s3Service{}
	//GetObject(in *pb.GetObjectInput, out *pb.Object) S3Error
	dbMock.On("GetObject", &objInput, &outobj).Return(err).
		Once()
	dbMock.On("UpdateObject", &outobj).Return(err).
		Once()
	type dumb struct {
	}
	dmbs := dumb{}
	s3Resp := pb.BaseResponse{"200", "success", dmbs, nil, 0}
	argsTmp:=args{context.Background(),&outobj,&s3Resp}
	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"createObjectTest1",b,argsTmp,false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.CreateObject(tt.args.ctx, tt.args.in, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.CreateObject() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_s3Service_UpdateObject(t *testing.T) {

	dbMock := new(mocks.DBAdapter)
	db.DbAdapter = dbMock
	type args struct {
		ctx context.Context
		in  *pb.Object
		out *pb.BaseResponse
	}
	b := &s3Service{}
	err:=S3Error{}
	err.Code=200
	inObj:=pb.Object{}
	dbMock.On("UpdateObject", &inObj).Return(err).
		Once()
	type dumb struct {
	}
	dmbs := dumb{}
	s3Resp := pb.BaseResponse{"200", "success", dmbs, nil, 0}
	argsTmp:=args{context.Background(),&inObj,&s3Resp}
	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"updateObjectTest",b,argsTmp,false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.UpdateObject(tt.args.ctx, tt.args.in, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.UpdateObject() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
func Test_s3Service_UpdateObject2(t *testing.T) {

	dbMock := new(mocks.DBAdapter)
	db.DbAdapter = dbMock
	type args struct {
		ctx context.Context
		in  *pb.Object
		out *pb.BaseResponse
	}
	b := &s3Service{}
	err:=S3Error{}
	err.Code=201
	inObj:=pb.Object{}
	dbMock.On("UpdateObject", &inObj).Return(err).
		Once()
	type dumb struct {
	}
	dmbs := dumb{}
	s3Resp := pb.BaseResponse{"200", "success", dmbs, nil, 0}
	argsTmp:=args{context.Background(),&inObj,&s3Resp}
	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"updateObjectTest",b,argsTmp,true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.UpdateObject(tt.args.ctx, tt.args.in, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.UpdateObject() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_s3Service_GetObject(t *testing.T) {
	type args struct {
		ctx context.Context
		in  *pb.GetObjectInput
		out *pb.Object
	}

	dbMock := new(mocks.DBAdapter)
	db.DbAdapter = dbMock

	err:=S3Error{}
	err.Code=200
	outobj:=pb.Object{}
	//outobj.BucketName="gsnbkt1"
	in:= pb.Bucket{}
	in.Name="gsnbkt1"
	in.Deleted=false
	objInput:=pb.GetObjectInput{}
	//objInput.Bucket="gsnbkt1"
	//objInput.Key="gsnbkt_obj"
	b := &s3Service{}
	//GetObject(in *pb.GetObjectInput, out *pb.Object) S3Error
	dbMock.On("GetObject", &objInput, &outobj).Return(err).
		Once()


	argsTmp:=args{context.Background(),&objInput,&outobj}

	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"getObjectEst",b,argsTmp,false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.GetObject(tt.args.ctx, tt.args.in, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.GetObject() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
func Test_s3Service_GetObject2(t *testing.T) {
	type args struct {
		ctx context.Context
		in  *pb.GetObjectInput
		out *pb.Object
	}

	dbMock := new(mocks.DBAdapter)
	db.DbAdapter = dbMock

	err:=S3Error{}
	err.Code=201
	outobj:=pb.Object{}
	//outobj.BucketName="gsnbkt1"
	in:= pb.Bucket{}
	in.Name="gsnbkt1"
	in.Deleted=false
	objInput:=pb.GetObjectInput{}
	//objInput.Bucket="gsnbkt1"
	//objInput.Key="gsnbkt_obj"
	b := &s3Service{}
	//GetObject(in *pb.GetObjectInput, out *pb.Object) S3Error
	dbMock.On("GetObject", &objInput, &outobj).Return(err).
		Once()


	argsTmp:=args{context.Background(),&objInput,&outobj}

	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"getObjectEst",b,argsTmp,false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.GetObject(tt.args.ctx, tt.args.in, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.GetObject() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_s3Service_DeleteObject(t *testing.T) {
	type args struct {
		ctx context.Context
		in  *pb.DeleteObjectInput
		out *pb.BaseResponse
	}
	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.DeleteObject(tt.args.ctx, tt.args.in, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.DeleteObject() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_s3Service_DeleteBucketLifecycle(t *testing.T) {
	dbMock := new(mocks.DBAdapter)
	db.DbAdapter = dbMock

	b := &s3Service{}
	err:=S3Error{}
	err.Code=200

	delLifcycleInput:=pb.DeleteLifecycleInput{}
	delLifcycleInput.Bucket=""
	delLifcycleInput.RuleID="rId"

	dbMock.On("DeleteBucketLifecycle", &delLifcycleInput).Return(err).
		Once()
	type dumb struct {
	}
	dmbs := dumb{}
	s3Resp := pb.BaseResponse{"200", "success", dmbs, nil, 0}




	type args struct {
		ctx context.Context
		in  *pb.DeleteLifecycleInput
		out *pb.BaseResponse
	}
	argsTmp:=args{context.Background(),&delLifcycleInput,&s3Resp}
	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"delObjTest",b,argsTmp,false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.DeleteBucketLifecycle(tt.args.ctx, tt.args.in, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.DeleteBucketLifecycle() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
func Test_s3Service_DeleteBucketLifecycle2(t *testing.T) {
	dbMock := new(mocks.DBAdapter)
	db.DbAdapter = dbMock

	b := &s3Service{}
	err:=S3Error{}
	err.Code=201

	delLifcycleInput:=pb.DeleteLifecycleInput{}
	delLifcycleInput.Bucket=""
	delLifcycleInput.RuleID="rId"

	dbMock.On("DeleteBucketLifecycle", &delLifcycleInput).Return(err).
		Once()
	type dumb struct {
	}
	dmbs := dumb{}
	s3Resp := pb.BaseResponse{"200", "success", dmbs, nil, 0}




	type args struct {
		ctx context.Context
		in  *pb.DeleteLifecycleInput
		out *pb.BaseResponse
	}
	argsTmp:=args{context.Background(),&delLifcycleInput,&s3Resp}
	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"delObjTest",b,argsTmp,true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.DeleteBucketLifecycle(tt.args.ctx, tt.args.in, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.DeleteBucketLifecycle() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
func Test_s3Service_UpdateBucket(t *testing.T) {
	type args struct {
		ctx context.Context
		in  *pb.Bucket
		out *pb.BaseResponse
	}

	dbMock := new(mocks.DBAdapter)
	db.DbAdapter = dbMock

	err:=S3Error{}
	err.Code=200

	in:= pb.Bucket{}
	in.Name="gsnbkt1"
	in.Deleted=false

	dbMock.On("UpdateBucket", &in).Return(err).
		Once()
	b := &s3Service{}
	type dumb struct {
	}
	dmbs := dumb{}
	s3Resp := pb.BaseResponse{"200", "success", dmbs, nil, 0}
	argTmp:=args{context.Background(),&in,&s3Resp}
	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"ugdateBktTest",b,argTmp,false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.UpdateBucket(tt.args.ctx, tt.args.in, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.UpdateBucket() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
func Test_s3Service_UpdateBucket2(t *testing.T) {
	type args struct {
		ctx context.Context
		in  *pb.Bucket
		out *pb.BaseResponse
	}

	dbMock := new(mocks.DBAdapter)
	db.DbAdapter = dbMock

	err:=S3Error{}
	err.Code=201

	in:= pb.Bucket{}
	in.Name="gsnbkt1"
	in.Deleted=false

	dbMock.On("UpdateBucket", &in).Return(err).
		Once()
	b := &s3Service{}
	type dumb struct {
	}
	dmbs := dumb{}
	s3Resp := pb.BaseResponse{"200", "success", dmbs, nil, 0}
	argTmp:=args{context.Background(),&in,&s3Resp}
	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"ugdateBktTest",b,argTmp,false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.UpdateBucket(tt.args.ctx, tt.args.in, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.UpdateBucket() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
func TestNewS3Service(t *testing.T) {
	tests := []struct {
		name string
		want pb.S3Handler
	}{
		// TODO: Add test cases.
		{"newS3ServiceTest",nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewS3Service(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewS3Service() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_s3Service_GetTierMap(t *testing.T) {
	type args struct {
		ctx context.Context
		in  *pb.BaseRequest
		out *pb.GetTierMapResponse
	}

	t2n := make(Int2String)
	t2n[Tier1] = AWS_STANDARD
	t2n[Tier99] = AWS_STANDARD_IA
	t2n[Tier999] = AWS_GLACIER

	Int2ExtTierMap = make(map[string]*Int2String)
	(Int2ExtTierMap)[OSTYPE_OPENSDS] = &t2n
	loadDefaultTransition()
	baseReq:=pb.BaseRequest{}
	outResp:=pb.GetTierMapResponse{}
	argTmp:=args{context.Background(),&baseReq,&outResp}
	b := &s3Service{}

	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"GetTierMpTest",b,argTmp,false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.GetTierMap(tt.args.ctx, tt.args.in, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.GetTierMap() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_s3Service_UpdateObjMeta(t *testing.T) {
	type args struct {
		ctx context.Context
		in  *pb.UpdateObjMetaRequest
		out *pb.BaseResponse
	}
	dbMock := new(mocks.DBAdapter)
	db.DbAdapter = dbMock
	err:=S3Error{}
	err.Code=200
	x:="gsnbkt1"
	y:=""
	var z int64=0
	storageMap:=map[string]interface {}{"storageclass":"STANDARD_IA", "tier":99}
	dbMock.On("UpdateObjMeta", &y,&x,z,storageMap).Return(err).
		Once()
	updateObjMetReq:=pb.UpdateObjMetaRequest{}
	updateObjMetReq.BucketName=x
	updateObjMetReq.ObjKey=y
	t2n := make(Int2String)
	t2n[Tier1] = AWS_STANDARD
	t2n[Tier99] = AWS_STANDARD_IA
	t2n[Tier999] = AWS_GLACIER

	Int2ExtTierMap = make(map[string]*Int2String)
	(Int2ExtTierMap)[OSTYPE_OPENSDS] = &t2n
	loadDefaultTransition()

	settings := make(map[string]string)

	settings["tier"]="99"
	updateObjMetReq.Setting=settings
	b := &s3Service{}
	type dumb struct {
	}
	dmbs := dumb{}
	s3Resp := pb.BaseResponse{"200", "success", dmbs, nil, 0}
	argTmp:=args{context.Background(),&updateObjMetReq,&s3Resp}

	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"updateObjMetaTest",b,argTmp,false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.UpdateObjMeta(tt.args.ctx, tt.args.in, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.UpdateObjMeta() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCheckReqObjMeta(t *testing.T) {
	type args struct {
		req   map[string]string
		valid map[string]struct{}
	}
	settings := make(map[string]string)

	settings["tier"]="99"
	valid := make(map[string]struct{})
	valid["tier"] = struct{}{}
	valid["backend"] = struct{}{}

	//validMap := make(map[string]struct{})

	//validMap["tier"]="99"

	ret := make(map[string]interface{})

	//ret["tier"]="99"

	err:=S3Error{}
	err.Code=500

	argTmp:=args{settings,valid}
	tests := []struct {
		name  string
		args  args
		want  map[string]interface{}
		want1 S3Error
	}{
		// TODO: Add test cases
		{"checkObjTest",argTmp,ret,err},

	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := CheckReqObjMeta(tt.args.req, tt.args.valid)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CheckReqObjMeta() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("CheckReqObjMeta() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_s3Service_GetBackendTypeByTier(t *testing.T) {
	type args struct {
		ctx context.Context
		in  *pb.GetBackendTypeByTierRequest
		out *pb.GetBackendTypeByTierResponse
	}
	req:=pb.GetBackendTypeByTierRequest{}
	req.Tier=99
	res:=pb.GetBackendTypeByTierResponse{}
	res.Types=[]string{AWS_STANDARD,AWS_STANDARD_IA, AWS_GLACIER}
	argTmp:=args{context.Background(),&req,&res}
	b := &s3Service{}
	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"getBkndTest1",b,argTmp,false},
	}
	t2n := make(Int2String)
	t2n[Tier1] = AWS_STANDARD
	t2n[Tier99] = AWS_STANDARD_IA
	t2n[Tier999] = AWS_GLACIER

	Int2ExtTierMap = make(map[string]*Int2String)
	(Int2ExtTierMap)[OSTYPE_OPENSDS] = &t2n
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.GetBackendTypeByTier(tt.args.ctx, tt.args.in, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.GetBackendTypeByTier() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_s3Service_AddUploadRecord(t *testing.T) {
	type args struct {
		ctx    context.Context
		record *pb.MultipartUploadRecord
		out    *pb.BaseResponse
	}
	record:=pb.MultipartUploadRecord{}
	dbMock := new(mocks.DBAdapter)
	db.DbAdapter = dbMock
	err:=S3Error{}
	err.Code=200
	dbMock.On("AddMultipartUpload", &record).Return(err).
		Once()

	type dumb struct {
	}
	dmbs := dumb{}
	s3Resp := pb.BaseResponse{"200", "success", dmbs, nil, 0}
	b := &s3Service{}
	argTmp:=args{context.Background(),&record,&s3Resp}
	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"addUploadTest1",b,argTmp,false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.AddUploadRecord(tt.args.ctx, tt.args.record, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.AddUploadRecord() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
func Test_s3Service_AddUploadRecord2(t *testing.T) {
	type args struct {
		ctx    context.Context
		record *pb.MultipartUploadRecord
		out    *pb.BaseResponse
	}
	record:=pb.MultipartUploadRecord{}
	dbMock := new(mocks.DBAdapter)
	db.DbAdapter = dbMock
	err:=S3Error{}
	err.Code=201
	dbMock.On("AddMultipartUpload", &record).Return(err).
		Once()

	type dumb struct {
	}
	dmbs := dumb{}
	s3Resp := pb.BaseResponse{"200", "success", dmbs, nil, 0}
	b := &s3Service{}
	argTmp:=args{context.Background(),&record,&s3Resp}
	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"addUploadTest1",b,argTmp,true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.AddUploadRecord(tt.args.ctx, tt.args.record, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.AddUploadRecord() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
func Test_s3Service_DeleteUploadRecord(t *testing.T) {
	type args struct {
		ctx    context.Context
		record *pb.MultipartUploadRecord
		out    *pb.BaseResponse
	}
	record:=pb.MultipartUploadRecord{}
	dbMock := new(mocks.DBAdapter)
	db.DbAdapter = dbMock
	err:=S3Error{}
	err.Code=200
	dbMock.On("DeleteMultipartUpload", &record).Return(err).
		Once()

	type dumb struct {
	}
	dmbs := dumb{}
	s3Resp := pb.BaseResponse{"200", "success", dmbs, nil, 0}
	b := &s3Service{}
	argTmp:=args{context.Background(),&record,&s3Resp}
	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"deleteUploadTest1",b,argTmp,false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.DeleteUploadRecord(tt.args.ctx, tt.args.record, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.DeleteUploadRecord() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
func Test_s3Service_DeleteUploadRecord2(t *testing.T) {
	type args struct {
		ctx    context.Context
		record *pb.MultipartUploadRecord
		out    *pb.BaseResponse
	}
	record:=pb.MultipartUploadRecord{}
	dbMock := new(mocks.DBAdapter)
	db.DbAdapter = dbMock
	err:=S3Error{}
	err.Code=201
	dbMock.On("DeleteMultipartUpload", &record).Return(err).
		Once()

	type dumb struct {
	}
	dmbs := dumb{}
	s3Resp := pb.BaseResponse{"200", "success", dmbs, nil, 0}
	b := &s3Service{}
	argTmp:=args{context.Background(),&record,&s3Resp}
	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"deleteUploadTest1",b,argTmp,true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.DeleteUploadRecord(tt.args.ctx, tt.args.record, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.DeleteUploadRecord() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_s3Service_ListUploadRecord(t *testing.T) {
	type args struct {
		ctx context.Context
		in  *pb.ListMultipartUploadRequest
		out *pb.ListMultipartUploadResponse
	}
	in:=pb.ListMultipartUploadRequest{}
	out:=pb.ListMultipartUploadResponse{}
	b := &s3Service{}
	argTmp:=args{context.Background(),&in,&out}

	dbMock := new(mocks.DBAdapter)
	db.DbAdapter = dbMock
	//record:=pb.MultipartUploadRecord{}
	records := []pb.MultipartUploadRecord{}
	err:=S3Error{}
	err.Code=200
	dbMock.On("ListUploadRecords", &in,&records).Return(err).
		Once()

	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"listUploadTest",b,argTmp,false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.ListUploadRecord(tt.args.ctx, tt.args.in, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.ListUploadRecord() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
func Test_s3Service_ListUploadRecord2(t *testing.T) {
	type args struct {
		ctx context.Context
		in  *pb.ListMultipartUploadRequest
		out *pb.ListMultipartUploadResponse
	}
	in:=pb.ListMultipartUploadRequest{}
	out:=pb.ListMultipartUploadResponse{}
	b := &s3Service{}
	argTmp:=args{context.Background(),&in,&out}

	dbMock := new(mocks.DBAdapter)
	db.DbAdapter = dbMock
	//record:=pb.MultipartUploadRecord{}
	records := []pb.MultipartUploadRecord{}
	err:=S3Error{}
	err.Code=201
	dbMock.On("ListUploadRecords", &in,&records).Return(err).
		Once()

	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"listUploadTest",b,argTmp,true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.ListUploadRecord(tt.args.ctx, tt.args.in, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.ListUploadRecord() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_s3Service_CountObjects(t *testing.T) {
	type args struct {
		ctx context.Context
		in  *pb.ListObjectsRequest
		out *pb.CountObjectsResponse
	}
	in:=pb.ListObjectsRequest{}
	out:=pb.CountObjectsResponse{}
	b := &s3Service{}
	argTmp:=args{context.Background(),&in, &out}
	dbMock := new(mocks.DBAdapter)
	db.DbAdapter = dbMock
	err:=S3Error{}
	err.Code=200
	countInfo := ObjsCountInfo{}
	dbMock.On("CountObjects", &in,&countInfo).Return(err).
		Once()

	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"countObjTest1",b,argTmp,false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.CountObjects(tt.args.ctx, tt.args.in, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.CountObjects() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
func Test_s3Service_CountObjects2(t *testing.T) {
	type args struct {
		ctx context.Context
		in  *pb.ListObjectsRequest
		out *pb.CountObjectsResponse
	}
	in:=pb.ListObjectsRequest{}
	out:=pb.CountObjectsResponse{}
	b := &s3Service{}
	argTmp:=args{context.Background(),&in, &out}
	dbMock := new(mocks.DBAdapter)
	db.DbAdapter = dbMock
	err:=S3Error{}
	err.Code=201
	countInfo := ObjsCountInfo{}
	dbMock.On("CountObjects", &in,&countInfo).Return(err).
		Once()

	tests := []struct {
		name    string
		b       *s3Service
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"countObjTest1",b,argTmp,true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &s3Service{}
			if err := b.CountObjects(tt.args.ctx, tt.args.in, tt.args.out); (err != nil) != tt.wantErr {
				t.Errorf("s3Service.CountObjects() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}