package v2

import (
	http_client "github.com/huaweicloud/huaweicloud-sdk-go-v3/core"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/services/evs/v2/model"
)

type EvsClient struct {
	hcClient *http_client.HcHttpClient
}

func NewEvsClient(hcClient *http_client.HcHttpClient) *EvsClient {
	return &EvsClient{hcClient: hcClient}
}

func EvsClientBuilder() *http_client.HcHttpClientBuilder {
	builder := http_client.NewHcHttpClientBuilder()
	return builder
}

//为指定云硬盘批量添加标签。  添加标签时，如果云硬盘的标签已存在相同key，则会覆盖已有标签。 单个云硬盘最多支持创建10个标签。
func (c *EvsClient) BatchCreateVolumeTags(request *model.BatchCreateVolumeTagsRequest) (*model.BatchCreateVolumeTagsResponse, error) {
	requestDef := GenReqDefForBatchCreateVolumeTags(request)
	resp, responseDef := GenRespForBatchCreateVolumeTags()

	if _, err := c.hcClient.Sync(request, requestDef, responseDef); err != nil {
		return nil, err
	} else {
		return resp, nil
	}
}

//为指定云硬盘批量删除标签。
func (c *EvsClient) BatchDeleteVolumeTags(request *model.BatchDeleteVolumeTagsRequest) (*model.BatchDeleteVolumeTagsResponse, error) {
	requestDef := GenReqDefForBatchDeleteVolumeTags(request)
	resp, responseDef := GenRespForBatchDeleteVolumeTags()

	if _, err := c.hcClient.Sync(request, requestDef, responseDef); err != nil {
		return nil, err
	} else {
		return resp, nil
	}
}

//将系统盘或数据盘的数据导出为IMS镜像，导出的镜像在IMS的私有镜像列表中可以查 看并使用。
func (c *EvsClient) CinderExportToImage(request *model.CinderExportToImageRequest) (*model.CinderExportToImageResponse, error) {
	requestDef := GenReqDefForCinderExportToImage(request)
	resp, responseDef := GenRespForCinderExportToImage()

	if _, err := c.hcClient.Sync(request, requestDef, responseDef); err != nil {
		return nil, err
	} else {
		return resp, nil
	}
}

//查询所有的可用分区信息。
func (c *EvsClient) CinderListAvailabilityZones(request *model.CinderListAvailabilityZonesRequest) (*model.CinderListAvailabilityZonesResponse, error) {
	requestDef := GenReqDefForCinderListAvailabilityZones(request)
	resp, responseDef := GenRespForCinderListAvailabilityZones()

	if _, err := c.hcClient.Sync(request, requestDef, responseDef); err != nil {
		return nil, err
	} else {
		return resp, nil
	}
}

//查询租户的详细配额。
func (c *EvsClient) CinderListQuotas(request *model.CinderListQuotasRequest) (*model.CinderListQuotasResponse, error) {
	requestDef := GenReqDefForCinderListQuotas(request)
	resp, responseDef := GenRespForCinderListQuotas()

	if _, err := c.hcClient.Sync(request, requestDef, responseDef); err != nil {
		return nil, err
	} else {
		return resp, nil
	}
}

//查询云硬盘类型列表。
func (c *EvsClient) CinderListVolumeTypes(request *model.CinderListVolumeTypesRequest) (*model.CinderListVolumeTypesResponse, error) {
	requestDef := GenReqDefForCinderListVolumeTypes(request)
	resp, responseDef := GenRespForCinderListVolumeTypes()

	if _, err := c.hcClient.Sync(request, requestDef, responseDef); err != nil {
		return nil, err
	} else {
		return resp, nil
	}
}

//创建云硬盘快照。
func (c *EvsClient) CreateSnapshot(request *model.CreateSnapshotRequest) (*model.CreateSnapshotResponse, error) {
	requestDef := GenReqDefForCreateSnapshot(request)
	resp, responseDef := GenRespForCreateSnapshot()

	if _, err := c.hcClient.Sync(request, requestDef, responseDef); err != nil {
		return nil, err
	} else {
		return resp, nil
	}
}

//创建按需或包周期云硬盘。 在创建包周期云硬盘的场景下： - 如果您需要查看订单可用的优惠券，请参考\"[查询订单可用优惠券](https://support.huaweicloud.com/api-oce/zh-cn_topic_0092953630.html)\"。 - 如果您需要支付订单，请参考\"[支付包周期产品订单](https://support.huaweicloud.com/api-oce/zh-cn_topic_0075746561.html)\"。 - 如果您需要查询订单的资源开通详情，请参考\"[查询订单的资源开通详情](https://support.huaweicloud.com/api-oce/api_order_00001.html)\"。 - 如果您需要退订该包周期资源，请参考“[退订包周期资源](https://support.huaweicloud.com/api-oce/zh-cn_topic_0082522030.html)”。
func (c *EvsClient) CreateVolume(request *model.CreateVolumeRequest) (*model.CreateVolumeResponse, error) {
	requestDef := GenReqDefForCreateVolume(request)
	resp, responseDef := GenRespForCreateVolume()

	if _, err := c.hcClient.Sync(request, requestDef, responseDef); err != nil {
		return nil, err
	} else {
		return resp, nil
	}
}

//删除云硬盘快照。
func (c *EvsClient) DeleteSnapshot(request *model.DeleteSnapshotRequest) (*model.DeleteSnapshotResponse, error) {
	requestDef := GenReqDefForDeleteSnapshot(request)
	resp, responseDef := GenRespForDeleteSnapshot()

	if _, err := c.hcClient.Sync(request, requestDef, responseDef); err != nil {
		return nil, err
	} else {
		return resp, nil
	}
}

//删除一个云硬盘。
func (c *EvsClient) DeleteVolume(request *model.DeleteVolumeRequest) (*model.DeleteVolumeResponse, error) {
	requestDef := GenReqDefForDeleteVolume(request)
	resp, responseDef := GenRespForDeleteVolume()

	if _, err := c.hcClient.Sync(request, requestDef, responseDef); err != nil {
		return nil, err
	} else {
		return resp, nil
	}
}

//查询云硬盘快照详细列表信息。
func (c *EvsClient) ListSnapshots(request *model.ListSnapshotsRequest) (*model.ListSnapshotsResponse, error) {
	requestDef := GenReqDefForListSnapshots(request)
	resp, responseDef := GenRespForListSnapshots()

	if _, err := c.hcClient.Sync(request, requestDef, responseDef); err != nil {
		return nil, err
	} else {
		return resp, nil
	}
}

//获取某个租户的所有云硬盘资源的标签信息。
func (c *EvsClient) ListVolumeTags(request *model.ListVolumeTagsRequest) (*model.ListVolumeTagsResponse, error) {
	requestDef := GenReqDefForListVolumeTags(request)
	resp, responseDef := GenRespForListVolumeTags()

	if _, err := c.hcClient.Sync(request, requestDef, responseDef); err != nil {
		return nil, err
	} else {
		return resp, nil
	}
}

//查询所有云硬盘的详细信息。
func (c *EvsClient) ListVolumes(request *model.ListVolumesRequest) (*model.ListVolumesResponse, error) {
	requestDef := GenReqDefForListVolumes(request)
	resp, responseDef := GenRespForListVolumes()

	if _, err := c.hcClient.Sync(request, requestDef, responseDef); err != nil {
		return nil, err
	} else {
		return resp, nil
	}
}

//通过标签查询云硬盘资源实例详情。
func (c *EvsClient) ListVolumesByTags(request *model.ListVolumesByTagsRequest) (*model.ListVolumesByTagsResponse, error) {
	requestDef := GenReqDefForListVolumesByTags(request)
	resp, responseDef := GenRespForListVolumesByTags()

	if _, err := c.hcClient.Sync(request, requestDef, responseDef); err != nil {
		return nil, err
	} else {
		return resp, nil
	}
}

//对按需或者包周期云硬盘进行扩容。 在扩容包周期云硬盘的场景下： - 如果您需要查看订单可用的优惠券，请参考\"[查询订单可用优惠券](https://support.huaweicloud.com/api-oce/zh-cn_topic_0092953630.html)\"。 - 如果您需要支付订单，请参考\"[支付包周期产品订单](https://support.huaweicloud.com/api-oce/zh-cn_topic_0075746561.html)\"。 - 如果您需要查询订单的资源开通详情，请参考\"[查询订单的资源开通详情](https://support.huaweicloud.com/api-oce/api_order_00001.html)\"。 - 如果您需要退订该包周期资源，请参考“[退订包周期资源](https://support.huaweicloud.com/api-oce/zh-cn_topic_0082522030.html)”。
func (c *EvsClient) ResizeVolume(request *model.ResizeVolumeRequest) (*model.ResizeVolumeResponse, error) {
	requestDef := GenReqDefForResizeVolume(request)
	resp, responseDef := GenRespForResizeVolume()

	if _, err := c.hcClient.Sync(request, requestDef, responseDef); err != nil {
		return nil, err
	} else {
		return resp, nil
	}
}

//将快照数据回滚到云硬盘。支持企业项目授权功能。
func (c *EvsClient) RollbackSnapshot(request *model.RollbackSnapshotRequest) (*model.RollbackSnapshotResponse, error) {
	requestDef := GenReqDefForRollbackSnapshot(request)
	resp, responseDef := GenRespForRollbackSnapshot()

	if _, err := c.hcClient.Sync(request, requestDef, responseDef); err != nil {
		return nil, err
	} else {
		return resp, nil
	}
}

//查询Job的执行状态。 可用于查询创建云硬盘，扩容云硬盘，删除云硬盘等API的执行状态。
func (c *EvsClient) ShowJob(request *model.ShowJobRequest) (*model.ShowJobResponse, error) {
	requestDef := GenReqDefForShowJob(request)
	resp, responseDef := GenRespForShowJob()

	if _, err := c.hcClient.Sync(request, requestDef, responseDef); err != nil {
		return nil, err
	} else {
		return resp, nil
	}
}

//查询单个云硬盘快照信息。支持企业项目授权功能。
func (c *EvsClient) ShowSnapshot(request *model.ShowSnapshotRequest) (*model.ShowSnapshotResponse, error) {
	requestDef := GenReqDefForShowSnapshot(request)
	resp, responseDef := GenRespForShowSnapshot()

	if _, err := c.hcClient.Sync(request, requestDef, responseDef); err != nil {
		return nil, err
	} else {
		return resp, nil
	}
}

//查询单个云硬盘的详细信息。支持企业项目授权功能。
func (c *EvsClient) ShowVolume(request *model.ShowVolumeRequest) (*model.ShowVolumeResponse, error) {
	requestDef := GenReqDefForShowVolume(request)
	resp, responseDef := GenRespForShowVolume()

	if _, err := c.hcClient.Sync(request, requestDef, responseDef); err != nil {
		return nil, err
	} else {
		return resp, nil
	}
}

//查询指定云硬盘的标签信息。
func (c *EvsClient) ShowVolumeTags(request *model.ShowVolumeTagsRequest) (*model.ShowVolumeTagsResponse, error) {
	requestDef := GenReqDefForShowVolumeTags(request)
	resp, responseDef := GenRespForShowVolumeTags()

	if _, err := c.hcClient.Sync(request, requestDef, responseDef); err != nil {
		return nil, err
	} else {
		return resp, nil
	}
}

//更新云硬盘快照。支持企业项目授权功能。
func (c *EvsClient) UpdateSnapshot(request *model.UpdateSnapshotRequest) (*model.UpdateSnapshotResponse, error) {
	requestDef := GenReqDefForUpdateSnapshot(request)
	resp, responseDef := GenRespForUpdateSnapshot()

	if _, err := c.hcClient.Sync(request, requestDef, responseDef); err != nil {
		return nil, err
	} else {
		return resp, nil
	}
}

//更新一个云硬盘的名称和描述。
func (c *EvsClient) UpdateVolume(request *model.UpdateVolumeRequest) (*model.UpdateVolumeResponse, error) {
	requestDef := GenReqDefForUpdateVolume(request)
	resp, responseDef := GenRespForUpdateVolume()

	if _, err := c.hcClient.Sync(request, requestDef, responseDef); err != nil {
		return nil, err
	} else {
		return resp, nil
	}
}
