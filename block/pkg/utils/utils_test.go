package utils

import (
	"context"
	"reflect"
	"testing"

	pstruct "github.com/golang/protobuf/ptypes/struct"

	backend "github.com/opensds/multi-cloud/backend/proto"
	"github.com/opensds/multi-cloud/block/pkg/model"
	pb "github.com/opensds/multi-cloud/block/proto"
	bkpb "github.com/opensds/multi-cloud/testutils/backend/proto"
	"github.com/opensds/multi-cloud/testutils/block/collection"
)

func TestUpdateVolumeStruct(t *testing.T) {
	type args struct {
		volModel *model.Volume
		volPb    *pb.Volume
	}

	volModel1 := &collection.SampleVolumes[0]
	volPb1 := &collection.SamplePBVolumes[0]
	volPb2 := &collection.SamplePBVolumes[1]

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{name: "TestUpdateVolumeStruct 1", args: args{volModel: volModel1, volPb: volPb1}, wantErr: false},
		{name: "TestUpdateVolumeStruct 2", args: args{volModel: volModel1, volPb: volPb2}, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := UpdateVolumeStruct(tt.args.volModel, tt.args.volPb); (err != nil) != tt.wantErr {
				t.Errorf("UpdateVolumeStruct() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestUpdateVolumeModel(t *testing.T) {
	type args struct {
		volPb    *pb.Volume
		volModel *model.Volume
	}

	volModel1 := &collection.SampleVolumes[0]
	volPb1 := &collection.SamplePBVolumes[0]
	volPb2 := &collection.SamplePBVolumes[1]

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{name: "TestUpdateVolumeModel 1", args: args{volModel: volModel1, volPb: volPb1}, wantErr: false},
		{name: "TestUpdateVolumeModel 2", args: args{volModel: volModel1, volPb: volPb2}, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := UpdateVolumeModel(tt.args.volPb, tt.args.volModel); (err != nil) != tt.wantErr {
				t.Errorf("UpdateVolumeModel() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestConvertTags(t *testing.T) {
	type args struct {
		pbtags []*pb.Tag
	}

	pbtag1 := pb.Tag{
		Key:   "T1",
		Value: "v1",
	}
	pbtag2 := pb.Tag{
		Key:   "T2",
		Value: "v2",
	}

	wantTag1 := model.Tag{Key: "T1", Value: "v1"}
	wantTag2 := model.Tag{Key: "T2", Value: "v2"}

	tests := []struct {
		name    string
		args    args
		want    []model.Tag
		wantErr bool
	}{
		{name: "TestConvertTags 1", args: args{pbtags: []*pb.Tag{&pbtag1}}, want: []model.Tag{wantTag1}, wantErr: false},
		{name: "TestConvertTags 2", args: args{pbtags: []*pb.Tag{&pbtag2}}, want: []model.Tag{wantTag2}, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ConvertTags(tt.args.pbtags)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConvertTags() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ConvertTags() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConvertMetadataStructToMap(t *testing.T) {
	type args struct {
		metaStruct *pstruct.Struct
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]interface{}
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ConvertMetadataStructToMap(tt.args.metaStruct)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConvertMetadataStructToMap() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ConvertMetadataStructToMap() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMergeVolumeData(t *testing.T) {
	type args struct {
		vol      *pb.Volume
		volFinal *pb.Volume
	}

	volPb1 := &collection.SamplePBVolumes[0]
	volPb2 := &collection.SamplePBVolumes[1]

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{name: "TestMergeVolumeData 1", args: args{vol: volPb1, volFinal: volPb2}, wantErr: false},
		{name: "TestMergeVolumeData 2", args: args{vol: volPb1, volFinal: volPb1}, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := MergeVolumeData(tt.args.vol, tt.args.volFinal); (err != nil) != tt.wantErr {
				t.Errorf("MergeVolumeData() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGetBackend(t *testing.T) {

	ctx := context.Background()

	bkendDetail := backend.BackendDetail{
		Id:                   "",
		TenantId:             "",
		UserId:               "",
		Name:                 "",
		Type:                 "",
		Region:               "",
		Endpoint:             "",
		BucketName:           "",
		Access:               "",
		Security:             "",
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	}

	bkendReq := &backend.GetBackendRequest{
		Id: "id",
	}

	bkendResp := backend.GetBackendResponse{
		Backend: &bkendDetail,
	}

	mockBackendClient := new(bkpb.BackendService)
	mockBackendClient.On("GetBackend", ctx, bkendReq).Return(&bkendResp, nil)

	GetBackend(ctx, mockBackendClient, "id")

}
