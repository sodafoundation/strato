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

package utils

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/opensds/multi-cloud/backend/proto"
	"github.com/opensds/multi-cloud/block/pkg/model"

	pstruct "github.com/golang/protobuf/ptypes/struct"
	pb "github.com/opensds/multi-cloud/block/proto"
	driverutils "github.com/opensds/multi-cloud/contrib/utils"
	log "github.com/sirupsen/logrus"
)

const (
	// It's RFC 8601 format that decodes and encodes with
	// exactly precision to seconds.
	TimeFormat = `2006-01-02T15:04:05`
)

const (
	// VolumeCreating is a VolumeState enum value
	VolumeStateCreating = "creating"

	// VolumeStateAvailable is a VolumeState enum value
	VolumeStateAvailable = "available"

	// VolumeStateInUse is a VolumeState enum value
	VolumeStateInUse = "inUse"

	// VolumeStateError is a VolumeState enum value
	VolumeStateError = "error"

	// VolumeStateUpdating is a VolumeState enum value
	VolumeStateUpdating = "updating"

	// VolumeStateDeleting is a VolumeState enum value
	VolumeStateDeleting = "deleting"

	// VolumeStateErrorDeleting is a VolumeState enum value
	VolumeStateErrorDeleting = "errorDeleting"

	// VolumeStateDeleted is a VolumeState enum value
	VolumeStateDeleted = "deleted"
)

var listFields map[string]*pstruct.Value

func GetBackend(ctx context.Context, backendClient backend.BackendService,
	backendId string) (*backend.GetBackendResponse, error) {
	backend, err := backendClient.GetBackend(ctx, &backend.GetBackendRequest{Id: backendId})
	if err != nil {
		log.Errorf("get backend %s failed.", backendId)
		return nil, err
	}
	log.Infof("backend response = [%+v]\n", backend)
	return backend, nil
}

func ParseStructFields(fields map[string]*pstruct.Value) (map[string]interface{}, error) {
	log.Infof("Parsing struct fields = [%+v]", fields)

	valuesMap := make(map[string]interface{})

	for key, value := range fields {
		if v, ok := value.GetKind().(*pstruct.Value_NullValue); ok {
			valuesMap[key] = v.NullValue
		} else if v, ok := value.GetKind().(*pstruct.Value_NumberValue); ok {
			valuesMap[key] = v.NumberValue
		} else if v, ok := value.GetKind().(*pstruct.Value_StringValue); ok {
			valuesMap[key] = strings.Trim(v.StringValue, "\"")
		} else if v, ok := value.GetKind().(*pstruct.Value_BoolValue); ok {
			valuesMap[key] = v.BoolValue
		} else if v, ok := value.GetKind().(*pstruct.Value_StructValue); ok {
			var err error
			valuesMap[key], err = ParseStructFields(v.StructValue.Fields)
			if err != nil {
				log.Errorf("Failed to parse struct Fields = [%+v] , err = %s", v.StructValue.Fields, err)
				return nil, err
			}
		} else if v, ok := value.GetKind().(*pstruct.Value_ListValue); ok {
			listFields[key] = v.ListValue.Values[0]
		} else {
			msg := fmt.Sprintf("Failed to parse field for key = [%s], value = [%+v]", key, value)
			err := errors.New(msg)
			log.Errorf(msg)
			return nil, err
		}
	}
	return valuesMap, nil
}

func UpdateVolumeStruct(volModel *model.Volume, volPb *pb.Volume) error {
	var tags []*pb.Tag
	for _, tag := range volModel.Tags {
		tags = append(tags, &pb.Tag{
			Key:   tag.Key,
			Value: tag.Value,
		})
	}
	volPb.Tags = tags

	metadata, err := driverutils.ConvertMapToStruct(volModel.Metadata)
	if err != nil {
		log.Error(err)
		return err
	}
	volPb.Metadata = metadata

	return nil
}

func UpdateVolumeModel(volPb *pb.Volume, volModel *model.Volume) error {

	tags, err := ConvertTags(volPb.Tags)
	if err != nil {
		log.Errorf("Failed to get conversions for tags: %v error = %s", volPb.Tags, err)
		return err
	}
	volModel.Tags = tags

	metadata := make(map[string]interface{})
	for k, v := range volModel.Metadata {
		metadata[k] = v
	}

	metaMap, err := ConvertMetadataStructToMap(volPb.Metadata)
	if err != nil {
		log.Errorf("Failed to get metaMap: %+v , error = %s", volPb.Metadata, err)
		return err
	}

	for k, v := range metaMap {
		metadata[k] = v
	}
	volModel.Metadata = metadata

	return nil
}

func ConvertTags(pbtags []*pb.Tag) ([]model.Tag, error) {
	var tags []model.Tag
	for _, tag := range pbtags {
		tags = append(tags, model.Tag{
			Key:   tag.Key,
			Value: tag.Value,
		})
	}
	return tags, nil
}

func ConvertMetadataStructToMap(metaStruct *pstruct.Struct) (map[string]interface{}, error) {

	var metaMap map[string]interface{}
	metaMap = make(map[string]interface{})
	listFields = make(map[string]*pstruct.Value)

	fields := metaStruct.GetFields()

	metaMap, err := ParseStructFields(fields)
	if err != nil {
		log.Errorf("Failed to get metadata: %+v , error = %s", fields, err)
		return metaMap, err
	}

	if len(listFields) != 0 {
		meta, err := ParseStructFields(listFields)
		if err != nil {
			log.Errorf("Failed to get array for metadata : %+v error = %s", listFields, err)
			return metaMap, err
		}
		for k, v := range meta {
			metaMap[k] = v
		}
	}
	return metaMap, nil
}

func MergeVolumeData(vol *pb.Volume, volFinal *pb.Volume) error {

	if vol.Tags != nil || len(vol.Tags) != 0 {
		volFinal.Tags = vol.Tags
	}
	var metaMapFinal map[string]interface{}

	metaMapFinal, err := ConvertMetadataStructToMap(volFinal.Metadata)
	if err != nil {
		log.Errorf("Failed to get metaMap: %+v , error = %s", volFinal.Metadata, err)
		return err
	}

	metaMap, err := ConvertMetadataStructToMap(vol.Metadata)
	if err != nil {
		log.Errorf("Failed to get metaMap: %+v, error = %s", vol.Metadata, err)
		return err
	}

	for k, v := range metaMap {
		metaMapFinal[k] = v
	}

	volFinal.Status = vol.Status
	volFinal.Size = vol.Size
	volFinal.Encrypted = vol.Encrypted
	volFinal.EncryptionSettings = vol.EncryptionSettings

	metadataFS, err := driverutils.ConvertMapToStruct(metaMapFinal)
	if err != nil {
		log.Error(err)
		return err
	}
	volFinal.Metadata = metadataFS

	return nil
}
