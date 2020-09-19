package collection

import (
	bModel "github.com/opensds/multi-cloud/block/pkg/model"
	bProto "github.com/opensds/multi-cloud/block/proto"
)

var isEncrypted = false
var size = int64(2000)
var iops = int64(3000)
var tags = []bModel.Tag{
	{
		Key:   "sample-key1",
		Value: "sample-value1",
	},
	{
		Key:   "sample-key2",
		Value: "sample-value2",
	},
}

var (
//[]*model.Volume


	SampleListPBVolumes = []*bProto.Volume{
	&SamplePBVolumes[0],
	&SamplePBVolumes[1],
	}

	SampleListModleVolumes= []*bModel.Volume{
		&SampleModelVolume1,
		&SampleModelVolume2,
	}

	SampleModelVolume1 = bModel.Volume{
			Id:                 "sample1-Id",
			CreatedAt:          "sample1-CreatedAt",
			UpdatedAt:          "sample1-UpdatedAt",
			TenantId:           "sample1-TenantId",
			UserId:             "sample1-UserId",
			BackendId:          "sample1-BackendId",
			Backend:            "sample1-Backend",
			Name:               "sample1-Name",
			Description:        "sample1-Description",
			Type:               "sample1-Type",
			Size:               &size,
			Region:             "sample1-Region",
			AvailabilityZone:   "sample1-AvailabilityZone",
			Status:             "sample1-Status",
			Iops:               iops,
			SnapshotId:         "SnapshotId",
			Tags:               tags,
			MultiAttach:        false,
			Encrypted:          &isEncrypted,
			EncryptionSettings: map[string]string{"key1":"val1"},
			Metadata:           nil,
		}

	SampleModelVolume2 = bModel.Volume{
		Id:                 "sample2-Id",
		CreatedAt:          "sample2-CreatedAt",
		UpdatedAt:          "sample2-UpdatedAt",
		TenantId:           "sample2-TenantId",
		UserId:             "sample2-UserId",
		BackendId:          "sample2-BackendId",
		Backend:            "sample2-Backend",
		Name:               "sample2-Name",
		Description:        "sample2-Description",
		Type:               "sample2-Type",
		Size:               &size,
		Region:             "sample2-Region",
		AvailabilityZone:   "sample2-AvailabilityZone",
		Status:             "sample2-Status",
		Iops:               iops,
		SnapshotId:         "SnapshotId",
		Tags:               tags,
		MultiAttach:        false,
		Encrypted:          &isEncrypted,
		EncryptionSettings: map[string]string{"key1":"val1"},
		Metadata:           nil,
	}

	SampleVolumes = []bModel.Volume{
		{
			Id:                 "sample1-Id",
			CreatedAt:          "sample1-CreatedAt",
			UpdatedAt:          "sample1-UpdatedAt",
			TenantId:           "sample1-TenantId",
			UserId:             "sample1-UserId",
			BackendId:          "sample1-BackendId",
			Backend:            "sample1-Backend",
			Name:               "sample1-Name",
			Description:        "sample1-Description",
			Type:               "sample1-Type",
			Size:               &size,
			Region:             "sample1-Region",
			AvailabilityZone:   "sample1-AvailabilityZone",
			Status:             "sample1-Status",
			Iops:               iops,
			SnapshotId:         "SnapshotId",
			Tags:               tags,
			MultiAttach:        false,
			Encrypted:          &isEncrypted,
			EncryptionSettings: map[string]string{"key1":"val1"},
			Metadata:           nil,
		},
		{
			Id:                 "sample2-Id",
			CreatedAt:          "sample2-CreatedAt",
			UpdatedAt:          "sample2-UpdatedAt",
			TenantId:           "sample2-TenantId",
			UserId:             "sample2-UserId",
			BackendId:          "sample2-BackendId",
			Backend:            "sample2-Backend",
			Name:               "sample2-Name",
			Description:        "sample2-Description",
			Type:               "sample2-Type",
			Size:               &size,
			Region:             "sample2-Region",
			AvailabilityZone:   "sample2-AvailabilityZone",
			Status:             "sample2-Status",
			Iops:               iops,
			SnapshotId:         "SnapshotId",
			Tags:               tags,
			MultiAttach:        false,
			Encrypted:          &isEncrypted,
			EncryptionSettings: map[string]string{"key1":"val1"},
			Metadata:           nil,
		},
	}


	SamplePBVolumes = []bProto.Volume{
		{
			Id:                 "pbsample1-Id",
			CreatedAt:          "pbsample1-CreatedAt",
			UpdatedAt:          "pbsample1-UpdatedAt",
			TenantId:           "pbsample1-TenantId",
			UserId:             "pbsample1-UserId",
			BackendId:          "pbsample1-BackendId",
			Backend:            "pbsample1-Backend",
			Name:               "pbsample1-Name",
			Description:        "pbsample1-Description",
			Type:               "pbsample1-Type",
			Size:               size,
			Region:             "pbsample1-Region",
			AvailabilityZone:   "pbsample1-AvailabilityZone",
			Status:             "pbsample1-Status",
			Iops:               iops,
			SnapshotId:         "SnapshotId1",

		},
		{
			Id:                 "pbsample2-Id",
			CreatedAt:          "pbsample2-CreatedAt",
			UpdatedAt:          "pbsample2-UpdatedAt",
			TenantId:           "pbsample2-TenantId",
			UserId:             "pbsample2-UserId",
			BackendId:          "pbsample2-BackendId",
			Backend:            "pbsample2-Backend",
			Name:               "pbsample2-Name",
			Description:        "pbsample2-Description",
			Type:               "pbsample2-Type",
			Size:               size,
			Region:             "pbsample2-Region",
			AvailabilityZone:   "pbsample2-AvailabilityZone",
			Status:             "pbsample2-Status",
			Iops:               iops,
			SnapshotId:         "SnapshotId2",

		},
	}
)
