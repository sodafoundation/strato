// Copyright 2020 The SODA Authors.
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


// This uses the  golang SDK by Huaweicloud and uses EVS client for all the ops

package hpc

import (
	"context"
	"errors"
	evs "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/evs/v2"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/services/evs/v2/model"
	"github.com/opensds/multi-cloud/block/proto"
	"github.com/opensds/multi-cloud/contrib/utils"
    "github.com/opensds/multi-cloud/contrib/datastore/block/common"
	log "github.com/sirupsen/logrus"
)

type HpcAdapter struct {
	evsc *evs.EvsClient
}

// This fucntion takes an object of VolumeDetail from the API result
// VolumeDetail is the base struct in the model for huaweicloud-sdk-go-v3 service model
func (ad *HpcAdapter) ParseVolume(evsVolResp *model.VolumeDetail) (*block.Volume, error) {
	//Create the map for metadata
    meta := make(map[string]interface{})
	meta = map[string]interface{}{
		common.VolumeId:              evsVolResp.Id,
		common.CreationTimeAtBackend: evsVolResp.CreatedAt,
	}
	metadata, err := utils.ConvertMapToStruct(meta)
	if err != nil {
		log.Errorf("failed to convert metadata = [%+v] to struct", metadata, err)
		return nil, err
	}


	size64 := (int64)(evsVolResp.Size)
	volume := &block.Volume{
		Name:     evsVolResp.Name,
		Size:     size64 * utils.GB_FACTOR,
		Status:   evsVolResp.Status,
		Type:     evsVolResp.VolumeType,
		Metadata: metadata,
	}
	return volume, nil
}

// This is the helper function which basically based upon the Volume Type provided by 
// API request, will return the corresponding CreateVolumeOptionVolumeType data of the SDK
func getVolumeTypeForHPC(volType string) (model.CreateVolumeOptionVolumeType, error) {
	if volType == "SAS" {
		return model.GetCreateVolumeOptionVolumeTypeEnum().SAS, nil
	} else if volType == "SATA" {
		return model.GetCreateVolumeOptionVolumeTypeEnum().SATA, nil
	} else if volType == "SSD" {
		return model.GetCreateVolumeOptionVolumeTypeEnum().SSD, nil
	} else if volType == "GPSSD" {
		return model.GetCreateVolumeOptionVolumeTypeEnum().GPSSD, nil
	} else {
		err := "wrong volumetype provided"
		return model.CreateVolumeOptionVolumeType{}, errors.New(err)
	}
}

// Create EVS volume
func (ad *HpcAdapter) CreateVolume(ctx context.Context, volume *block.CreateVolumeRequest) (*block.CreateVolumeResponse, error) {
	evsClient := ad.evsc

	availabilityZone := volume.Volume.AvailabilityZone

    // The SDK model expects the size to be int32,
	size := (int32)(volume.Volume.Size / utils.GB_FACTOR)

	volumeType, typeErr := getVolumeTypeForHPC(volume.Volume.Type)
	if typeErr != nil {
		log.Error(typeErr)
		return nil, typeErr
	}

	name := volume.Volume.Name

	createVolOption := &model.CreateVolumeOption{
		AvailabilityZone: availabilityZone,
		Name:             &name,
		Size:             &size,
		VolumeType:       volumeType,
	}

	volRequest := &model.CreateVolumeRequest{
		Body: &model.CreateVolumeRequestBody{
			Volume: createVolOption},
	}

    // CreateVolume fromt the EVS client takes CreateVolumeRequest and the
    // Type hieararchy is CreateVolumeRequest -> Volume (CreateVolumeOption) -> VolumeDetails
	result, err := evsClient.CreateVolume(volRequest)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	log.Debugf("Submitted job for volume creation. Job is %+v", result)

	// TODO: START
	// The job for Create Volume is submitted.
	// As of now there is no option to get the volume ID from the response
	// It only provides Job ID
	// FIXME: Making it to call list volumes and get the volume details

	listVolumes, err := ad.ListVolume(nil, nil)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	volumes := listVolumes.Volumes


	for _, volume := range volumes {
		if volume.Name == name {
	        log.Debugf("Create Volume response %+v", volume)
			return &block.CreateVolumeResponse{
				Volume: volume,
			}, nil
		}
	}
	// TODO: END
	return nil, errors.New("Error in creating volume")
}

// This will list all the volumes 
func (ad *HpcAdapter) ListVolume(ctx context.Context, volume *block.ListVolumeRequest) (*block.ListVolumeResponse, error) {
    log.Infof("Entered to List all volumes")
	evsClient := ad.evsc

	listVolRequest := &model.ListVolumesRequest{}
	response, volErr := evsClient.ListVolumes(listVolRequest)
	if volErr != nil {
		log.Error(volErr)
		return nil, volErr
	}

	var volumes []*block.Volume
	for _, resObj := range *response.Volumes {
		volume, parseErr := ad.ParseVolume(&resObj)
		if parseErr != nil {
			log.Error(parseErr)
			return nil, parseErr
		}
		volumes = append(volumes, volume)
	}

    log.Debugf("Listing volumes: %+v", volumes)
	return &block.ListVolumeResponse{
		Volumes: volumes,
	}, nil
}

// Get the details of a particular volume
func (ad *HpcAdapter) GetVolume(ctx context.Context, volume *block.GetVolumeRequest) (*block.GetVolumeResponse, error) {
    log.Infof("Entered to get the volume details of %s", volume.Volume.Name)
	evsClient := ad.evsc

    // Get the VolumeId from the metadata for the requested volume
	volumeId := volume.Volume.Metadata.Fields[common.VolumeId].GetStringValue()
	showRequest := &model.ShowVolumeRequest{VolumeId: volumeId}
	showResponse, showErr := evsClient.ShowVolume(showRequest)
	if showErr != nil {
		log.Error(showErr)
		return nil, showErr
	}

	log.Infof("Get volume response: %+v", showResponse)
	vol, parseErr := ad.ParseVolume(showResponse.Volume)
	if parseErr != nil {
		log.Error(parseErr)
		return nil, parseErr
	}

	return &block.GetVolumeResponse{
		Volume: vol,
	}, nil
}

// Update the name of a volume
func (ad *HpcAdapter) UpdateVolume(ctx context.Context, volume *block.UpdateVolumeRequest) (*block.UpdateVolumeResponse, error) {
	evsClient := ad.evsc

	log.Error("Entered to update volume with details: %+v", volume)
	name := volume.Volume.Name
	updateVolOption := &model.UpdateVolumeOption{
		Name: &name,
	}

	volumeId := volume.Volume.Metadata.Fields[common.VolumeId].GetStringValue()
	updateVolumeRequest := &model.UpdateVolumeRequest{
		VolumeId: volumeId,
		Body: &model.UpdateVolumeRequestBody{
			Volume: updateVolOption,
		},
	}

	updateResp, updateErr := evsClient.UpdateVolume(updateVolumeRequest)
	if updateErr != nil {
		log.Error(updateErr)
		return nil, updateErr
	}

	meta := map[string]interface{}{
		common.VolumeId: *updateResp.Id,
	}

	metadata, err := utils.ConvertMapToStruct(meta)
	if err != nil {
		log.Errorf("failed to convert metadata = [%+v] to struct", metadata, err)
		return nil, err
	}
	size := (int64)(*updateResp.Size * utils.GB_FACTOR)
	vol := &block.Volume{
		Size:     size,
		Status:   *updateResp.Status,
		Type:     *updateResp.VolumeType,
		Metadata: metadata,
	}

    log.Debugf("Volume updated details: %+v", vol)
	return &block.UpdateVolumeResponse{
		Volume: vol,
	}, nil
}

// Delete the volume with a particular volume ID
func (ad *HpcAdapter) DeleteVolume(ctx context.Context, volume *block.DeleteVolumeRequest) (*block.DeleteVolumeResponse, error) {
	volumeId := volume.Volume.Metadata.Fields[common.VolumeId].GetStringValue()

	delRequest := &model.DeleteVolumeRequest{VolumeId: volumeId}

	client := ad.evsc
	delResponse, delErr := client.DeleteVolume(delRequest)
	if delErr != nil {
		log.Error(delErr)
		return nil, delErr
	}
	log.Debugf("Delete volume response = %+v", delResponse)

	return &block.DeleteVolumeResponse{}, nil
}

func (ad *HpcAdapter) Close() error {
	// TODO:
	return nil
}
