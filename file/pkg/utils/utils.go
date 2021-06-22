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
	"strconv"
	"strings"

	backend "github.com/opensds/multi-cloud/backend/proto"
	"github.com/opensds/multi-cloud/file/pkg/model"

	pstruct "github.com/golang/protobuf/ptypes/struct"
	log "github.com/sirupsen/logrus"

	driverutils "github.com/opensds/multi-cloud/contrib/utils"
	pb "github.com/opensds/multi-cloud/file/proto"
)

const (
	// It's RFC 8601 format that decodes and encodes with
	// exactly precision to seconds.
	TimeFormat = `2006-01-02T15:04:05`

	AZURE_FILESHARE_USAGE_BYTES = "Share-Usage-Bytes"

	AZURE_X_MS_SHARE_QUOTA = "X-Ms-Share-Quota"

	CONTENT_LENGTH = "Content-Length"
)

const (
	// FileShareStateCreating is a FileShareState enum value
	FileShareStateCreating = "creating"

	// FileShareStateAvailable is a FileShareState enum value
	FileShareStateAvailable = "available"

	// FileShareStateInUse is a FileShareState enum value
	FileShareStateInUse = "inUse"

	// FileShareStateError is a FileShareState enum value
	FileShareStateError = "error"

	// FileShareStateUpdating is a FileShareState enum value
	FileShareStateUpdating = "updating"

	// FileShareStateDeleting is a FileShareState enum value
	FileShareStateDeleting = "deleting"

	// FileShareStateErrorDeleting is a FileShareState enum value
	FileShareStateErrorDeleting = "errorDeleting"

	// FileShareStateDeleted is a FileShareState enum value
	FileShareStateDeleted = "deleted"
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
			val := strings.Trim(v.StringValue, "\"")
			if key == AZURE_FILESHARE_USAGE_BYTES || key == AZURE_X_MS_SHARE_QUOTA || key == CONTENT_LENGTH {
				valInt, err := strconv.ParseInt(val, 10, 64)
				if err != nil {
					log.Errorf("Failed to parse string Field Key = %s , %s", key, err)
					return nil, err
				}
				valuesMap[key] = valInt
			} else {
				valuesMap[key] = val
			}
		} else if v, ok := value.GetKind().(*pstruct.Value_BoolValue); ok {
			valuesMap[key] = v.BoolValue
		} else if v, ok := value.GetKind().(*pstruct.Value_StructValue); ok {
			var err error
			valuesMap[key], err = ParseStructFields(v.StructValue.Fields)
			if err != nil {
				log.Errorf("Failed to parse struct Fields = [%+v] , %s", v.StructValue.Fields, err)
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

func UpdateFileShareStruct(fsModel *model.FileShare, fs *pb.FileShare) error {
	var tags []*pb.Tag
	for _, tag := range fsModel.Tags {
		tags = append(tags, &pb.Tag{
			Key:   tag.Key,
			Value: tag.Value,
		})
	}
	fs.Tags = tags

	metadata, err := driverutils.ConvertMapToStruct(fsModel.Metadata)
	if err != nil {
		log.Error(err)
		return err
	}
	fs.Metadata = metadata

	return nil
}

func UpdateFileShareModel(fs *pb.FileShare, fsModel *model.FileShare) error {

	tags, err := ConvertTags(fs.Tags)
	if err != nil {
		log.Errorf("Failed to get conversions for tags: %v , %s", fs.Tags, err)
		return err
	}
	fsModel.Tags = tags

	metadata := make(map[string]interface{})
	for k, v := range fsModel.Metadata {
		metadata[k] = v
	}

	metaMap, err := ConvertMetadataStructToMap(fs.Metadata)
	if err != nil {
		log.Errorf("Failed to get metaMap: %+v , %s", fs.Metadata, err)
		return err
	}

	for k, v := range metaMap {
		metadata[k] = v
	}
	fsModel.Metadata = metadata

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
		log.Errorf("Failed to get metadata: %+v, %s", fields, err)
		return metaMap, err
	}

	if len(listFields) != 0 {
		meta, err := ParseStructFields(listFields)
		if err != nil {
			log.Errorf("Failed to get array for metadata : %+v , %s", listFields, err)
			return metaMap, err
		}
		for k, v := range meta {
			metaMap[k] = v
		}
	}
	return metaMap, nil
}

func MergeFileShareData(fs *pb.FileShare, fsFinal *pb.FileShare) error {

	if fs.Tags != nil || len(fs.Tags) != 0 {
		fsFinal.Tags = fs.Tags
	}
	var metaMapFinal map[string]interface{}

	metaMapFinal, err := ConvertMetadataStructToMap(fsFinal.Metadata)
	if err != nil {
		log.Errorf("Failed to get metaMap: %+v , %s", fsFinal.Metadata, err)
		return err
	}

	metaMap, err := ConvertMetadataStructToMap(fs.Metadata)
	if err != nil {
		log.Errorf("Failed to get metaMap: %+v , %s", fs.Metadata, err)
		return err
	}

	for k, v := range metaMap {
		metaMapFinal[k] = v
	}

	fsFinal.Status = fs.Status
	fsFinal.Size = fs.Size
	fsFinal.Encrypted = fs.Encrypted
	fsFinal.EncryptionSettings = fs.EncryptionSettings

	metadataFS, err := driverutils.ConvertMapToStruct(metaMapFinal)
	if err != nil {
		log.Error(err)
		return err
	}
	fsFinal.Metadata = metadataFS

	return nil
}
