package ceph

import (
	"context"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/common"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/driver"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	pb "github.com/opensds/multi-cloud/s3/proto"
	"github.com/webrtcn/s3client"
	"io"
	"reflect"
	"testing"
)

func getbackend() *backendpb.BackendDetail {
	var bcknd backendpb.BackendDetail
	bcknd.Security = "eo2JKM5zE8PTghQuhRWnKZPVOrPQbW7ctJp3rxqt"
	bcknd.Access = "OCLEBXWF3238AIGFWCQ9"
	bcknd.Endpoint = "http://192.168.3.47:7480"
	bcknd.Type = "ceph-s3"
	bcknd.BucketName = "cephbucket"
	bcknd.Name = "ceph-test"
	bcknd.Id = "60893cb10de8710001657547"
	bcknd.TenantId = "94b280022d0c4401bcf3b0ea85870519"
	bcknd.Region = "-"
	bcknd.UserId = "558057c4256545bd8a307c37464003c9"
	return &bcknd
}
func getField() CephAdapter {
	bcknd := getbackend()
	endpoint := bcknd.Endpoint
	AccessKeyID := bcknd.Access
	AccessKeySecret := bcknd.Security
	sess := s3client.NewClient(endpoint, AccessKeyID, AccessKeySecret)
	value := &CephAdapter{backend: bcknd, session: sess}
	return *value
}

func TestCephAdapter_AbortMultipartUpload(t *testing.T) {
	var multipartObj pb.MultipartUpload
	multipartObj.ObjectId = ""
	multipartObj.Key = ""
	multipartObj.Bucket = ""
	multipartObj.Location = ""
	multipartObj.Tier = 0
	type args struct {
		ctx             context.Context
		multipartUpload *pb.MultipartUpload
	}
	tests := []struct {
		name    string
		fields  CephAdapter
		args    args
		wantErr bool
	}{
		{"abortMultipart", getField(), args{context.Background(), &multipartObj}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ad := &CephAdapter{
				backend: tt.fields.backend,
				session: tt.fields.session,
			}
			if err := ad.AbortMultipartUpload(tt.args.ctx, tt.args.multipartUpload); (err != nil) != tt.wantErr {
				t.Errorf("AbortMultipartUpload() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCephAdapter_BackendCheck(t *testing.T) {
	var bcknd pb.BackendDetailS3
	bcknd.Security = "eo2JKM5zE8PTghQuhRWnKZPVOrPQbW7ctJp3rxqt"
	bcknd.Access = "OCLEBXWF3238AIGFWCQ9"
	bcknd.Endpoint = "http://192.168.3.47:7480"
	bcknd.Type = "ceph-s3"
	bcknd.BucketName = "cephbucket"
	bcknd.Name = "ceph-test"
	bcknd.Id = "60893cb10de8710001657547"
	bcknd.TenantId = "94b280022d0c4401bcf3b0ea85870519"
	bcknd.Region = "-"
	bcknd.UserId = "558057c4256545bd8a307c37464003c9"
	type args struct {
		ctx           context.Context
		backendDetail *pb.BackendDetailS3
	}
	tests := []struct {
		name    string
		fields  CephAdapter
		args    args
		wantErr bool
	}{
		{"cephBackend", getField(), args{context.Background(), &bcknd}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ad := &CephAdapter{
				backend: tt.fields.backend,
				session: tt.fields.session,
			}
			if err := ad.BackendCheck(tt.args.ctx, tt.args.backendDetail); (err != nil) != tt.wantErr {
				t.Errorf("BackendCheck() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCephAdapter_BucketCreate(t *testing.T) {

	type args struct {
		ctx   context.Context
		input *pb.Bucket
	}
	tests := []struct {
		name    string
		fields  CephAdapter
		args    args
		wantErr bool
	}{
		{"createBucket", getField(), args{
			ctx: context.Background(),
			input: &pb.Bucket{
				Name: sampleBucket,
			},
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ad := &CephAdapter{
				backend: tt.fields.backend,
				session: tt.fields.session,
			}
			if err := ad.BucketCreate(tt.args.ctx, tt.args.input); (err != nil) != tt.wantErr {
				t.Errorf("BucketCreate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCephAdapter_BucketDelete(t *testing.T) {

	type args struct {
		ctx context.Context
		in  *pb.Bucket
	}
	tests := []struct {
		name    string
		fields  CephAdapter
		args    args
		wantErr bool
	}{
		{"deleteBucket", getField(), args{
			ctx: context.Background(),
			in: &pb.Bucket{
				Name: sampleBucket,
			},
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ad := &CephAdapter{
				backend: tt.fields.backend,
				session: tt.fields.session,
			}
			if err := ad.BucketDelete(tt.args.ctx, tt.args.in); (err != nil) != tt.wantErr {
				t.Errorf("BucketDelete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCephAdapter_ChangeStorageClass(t *testing.T) {
	//Input fields
	var obj pb.Object
	str := "GLACIER"
	obj.BucketName = "ceph-sm-1"
	obj.ObjectKey = "image.png"
	obj.Size = 718244

	type args struct {
		ctx      context.Context
		object   *pb.Object
		newClass *string
	}
	tests := []struct {
		name    string
		fields  CephAdapter
		args    args
		wantErr bool
	}{
		{"changeStorageclass", getField(), args{object: &obj, ctx: context.Background(), newClass: &str}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ad := &CephAdapter{
				backend: tt.fields.backend,
				session: tt.fields.session,
			}
			if err := ad.ChangeStorageClass(tt.args.ctx, tt.args.object, tt.args.newClass); (err != nil) != tt.wantErr {
				t.Errorf("ChangeStorageClass() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCephAdapter_Close(t *testing.T) {

	tests := []struct {
		name    string
		fields  CephAdapter
		wantErr bool
	}{
		{"Close", getField(), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ad := &CephAdapter{
				backend: tt.fields.backend,
				session: tt.fields.session,
			}
			if err := ad.Close(); (err != nil) != tt.wantErr {
				t.Errorf("Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCephAdapter_CompleteMultipartUpload(t *testing.T) {

	type args struct {
		ctx             context.Context
		multipartUpload *pb.MultipartUpload
		completeUpload  *model.CompleteMultipartUpload
	}
	tests := []struct {
		name    string
		fields  CephAdapter
		args    args
		want    *model.CompleteMultipartUploadResult
		wantErr bool
	}{
		//Todo Add test case
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ad := &CephAdapter{
				backend: tt.fields.backend,
				session: tt.fields.session,
			}
			got, err := ad.CompleteMultipartUpload(tt.args.ctx, tt.args.multipartUpload, tt.args.completeUpload)
			if (err != nil) != tt.wantErr {
				t.Errorf("CompleteMultipartUpload() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CompleteMultipartUpload() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCephAdapter_Copy(t *testing.T) {
	var output common.PutResult
	type args struct {
		ctx    context.Context
		stream io.Reader
		target *pb.Object
	}
	tests := []struct {
		name       string
		fields     CephAdapter
		args       args
		wantResult common.PutResult
		wantErr    bool
	}{
		{"copyTest", getField(), args{context.Background(), nil, nil}, output, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ad := &CephAdapter{
				backend: tt.fields.backend,
				session: tt.fields.session,
			}
			gotResult, err := ad.Copy(tt.args.ctx, tt.args.stream, tt.args.target)
			if (err != nil) != tt.wantErr {
				t.Errorf("Copy() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotResult, tt.wantResult) {
				t.Errorf("Copy() gotResult = %v, want %v", gotResult, tt.wantResult)
			}
		})
	}
}

func TestCephAdapter_Delete(t *testing.T) {
	var input pb.DeleteObjectInput
	input.Bucket = "ceph-sm-1"
	input.Key = "image.png"
	input.ObjectId = "image.png"
	input.ETag = "1a2bf29987450ab7ba5bb8b54a60d9d1"
	input.VersioId = "18446744072089864114"
	type args struct {
		ctx    context.Context
		object *pb.DeleteObjectInput
	}
	tests := []struct {
		name    string
		fields  CephAdapter
		args    args
		wantErr bool
	}{
		{"deleteObject", getField(), args{ctx: context.Background(), object: &input}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ad := &CephAdapter{
				backend: tt.fields.backend,
				session: tt.fields.session,
			}
			if err := ad.Delete(tt.args.ctx, tt.args.object); (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCephAdapter_Get(t *testing.T) {
	//Input param
	var obj pb.Object
	obj.BucketName = "ceph-sm-1"
	obj.ObjectKey = "image.png"
	obj.Size = 718244

	type args struct {
		ctx    context.Context
		object *pb.Object
		start  int64
		end    int64
	}
	tests := []struct {
		name    string
		fields  CephAdapter
		args    args
		want    io.ReadCloser
		wantErr bool
	}{
		{"getObject", getField(), args{context.Background(), &obj, 0, 718243},
			nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ad := &CephAdapter{
				backend: tt.fields.backend,
				session: tt.fields.session,
			}
			_, err := ad.Get(tt.args.ctx, tt.args.object, tt.args.start, tt.args.end)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestCephAdapter_InitMultipartUpload(t *testing.T) {
	type args struct {
		ctx    context.Context
		object *pb.Object
	}
	tests := []struct {
		name    string
		fields  CephAdapter
		args    args
		want    *pb.MultipartUpload
		wantErr bool
	}{
		//Todo write case here
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ad := &CephAdapter{
				backend: tt.fields.backend,
				session: tt.fields.session,
			}
			got, err := ad.InitMultipartUpload(tt.args.ctx, tt.args.object)
			if (err != nil) != tt.wantErr {
				t.Errorf("InitMultipartUpload() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("InitMultipartUpload() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCephAdapter_ListParts(t *testing.T) {
	type args struct {
		ctx             context.Context
		multipartUpload *pb.ListParts
	}
	tests := []struct {
		name    string
		fields  CephAdapter
		args    args
		want    *model.ListPartsOutput
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ad := &CephAdapter{
				backend: tt.fields.backend,
				session: tt.fields.session,
			}
			got, err := ad.ListParts(tt.args.ctx, tt.args.multipartUpload)
			if (err != nil) != tt.wantErr {
				t.Errorf("ListParts() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ListParts() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCephAdapter_Put(t *testing.T) {
	type args struct {
		ctx    context.Context
		stream io.Reader
		object *pb.Object
	}
	tests := []struct {
		name       string
		fields     CephAdapter
		args       args
		wantResult common.PutResult
		wantErr    bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ad := &CephAdapter{
				backend: tt.fields.backend,
				session: tt.fields.session,
			}
			gotResult, err := ad.Put(tt.args.ctx, tt.args.stream, tt.args.object)
			if (err != nil) != tt.wantErr {
				t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotResult, tt.wantResult) {
				t.Errorf("Put() gotResult = %v, want %v", gotResult, tt.wantResult)
			}
		})
	}
}

func TestCephAdapter_Restore(t *testing.T) {
	type args struct {
		ctx context.Context
		inp *pb.Restore
	}
	tests := []struct {
		name    string
		fields  CephAdapter
		args    args
		wantErr bool
	}{
		{"restoreTest", getField(), args{context.Background(), nil}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ad := &CephAdapter{
				backend: tt.fields.backend,
				session: tt.fields.session,
			}
			if err := ad.Restore(tt.args.ctx, tt.args.inp); (err != nil) != tt.wantErr {
				t.Errorf("Restore() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCephAdapter_UploadPart(t *testing.T) {
	type args struct {
		ctx             context.Context
		stream          io.Reader
		multipartUpload *pb.MultipartUpload
		partNumber      int64
		upBytes         int64
	}
	tests := []struct {
		name    string
		fields  CephAdapter
		args    args
		want    *model.UploadPartResult
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ad := &CephAdapter{
				backend: tt.fields.backend,
				session: tt.fields.session,
			}
			got, err := ad.UploadPart(tt.args.ctx, tt.args.stream, tt.args.multipartUpload, tt.args.partNumber, tt.args.upBytes)
			if (err != nil) != tt.wantErr {
				t.Errorf("UploadPart() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("UploadPart() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCephS3DriverFactory_CreateDriver(t *testing.T) {
	var wantdriver driver.StorageDriver
	adap := getField()
	wantdriver = &adap
	type args struct {
		backend *backendpb.BackendDetail
	}
	tests := []struct {
		name    string
		args    args
		want    driver.StorageDriver
		wantErr bool
	}{
		{"createBackend", args{getbackend()}, wantdriver, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cdf := &CephS3DriverFactory{}
			got, err := cdf.CreateDriver(tt.args.backend)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateDriver() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CreateDriver() got = %v, want %v", got, tt.want)
			}
		})
	}
}
