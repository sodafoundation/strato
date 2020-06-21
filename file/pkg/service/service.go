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

package service

import (
	"context"
	"errors"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"github.com/opensds/multi-cloud/api/pkg/common"
	"strings"
	"time"

	"github.com/micro/go-micro/v2/client"
	"github.com/opensds/multi-cloud/contrib/datastore/drivers"
	"github.com/opensds/multi-cloud/file/pkg/db"
	"github.com/opensds/multi-cloud/file/pkg/model"
	"github.com/opensds/multi-cloud/file/pkg/utils"

	pstruct "github.com/golang/protobuf/ptypes/struct"
	backend "github.com/opensds/multi-cloud/backend/proto"
	driverutils "github.com/opensds/multi-cloud/contrib/utils"
	pb "github.com/opensds/multi-cloud/file/proto"
	log "github.com/sirupsen/logrus"
)

var listFields map[string]*pstruct.Value

type fileService struct {
	fileClient    pb.FileService
	backendClient backend.BackendService
}

func NewFileService() pb.FileHandler {

	log.Infof("Init file service finished.\n")
	return &fileService{
		fileClient:    pb.NewFileService("file", client.DefaultClient),
		backendClient: backend.NewBackendService("backend", client.DefaultClient),
	}
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
			valuesMap[key] = val
		} else if v, ok := value.GetKind().(*pstruct.Value_BoolValue); ok {
			valuesMap[key] = v.BoolValue
		} else if v, ok := value.GetKind().(*pstruct.Value_StructValue); ok {
			var err error
			valuesMap[key], err = ParseStructFields(v.StructValue.Fields)
			if err != nil {
				log.Errorf("Failed to parse struct Fields = [%+v]", v.StructValue.Fields)
				return nil, err
			}
		} else if v, ok := value.GetKind().(*pstruct.Value_ListValue); ok {
			listFields[key] = v.ListValue.Values[0]
		} else {
			msg := fmt.Sprintf("Failed to parse field for key = [%+v], value = [%+v]", key, value)
			err := errors.New(msg)
			log.Errorf(msg)
			return nil, err
		}
	}
	return valuesMap, nil
}

func (f *fileService) UpdateFileShareStruct(fsModel *model.FileShare, fs *pb.FileShare) error {
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

func (f *fileService) UpdateFileShareModel(fs *pb.FileShare, fsModel *model.FileShare) error {
	var tags []model.Tag
	for _, tag := range fs.Tags {
		tags = append(tags, model.Tag{
			Key:   tag.Key,
			Value: tag.Value,
		})
	}
	fsModel.Tags = tags

	listFields = make(map[string]*pstruct.Value)

	fields := fs.Metadata.GetFields()

	metadata, err := ParseStructFields(fields)
	if err != nil {
		log.Errorf("Failed to get metadata: %v", err)
		return err
	}
	if len(listFields) != 0 {
		meta, err := ParseStructFields(listFields)
		if err != nil {
			log.Errorf("Failed to get array for metadata : %v", err)
			return err
		}
		for k, v := range meta {
			metadata[k] = v
		}
	}
	fsModel.Metadata = metadata

	return nil
}

func (f *fileService) ListFileShare(ctx context.Context, in *pb.ListFileShareRequest, out *pb.ListFileShareResponse) error {
	log.Info("Received ListFileShare request.")

	if in.Limit < 0 || in.Offset < 0 {
		msg := fmt.Sprintf("Invalid pagination parameter, limit = %d and offset = %d.", in.Limit, in.Offset)
		log.Info(msg)
		return errors.New(msg)
	}

	res, err := db.DbAdapter.ListFileShare(ctx, int(in.Limit), int(in.Offset), in.Filter)
	if err != nil {
		log.Errorf("Failed to List FileShares: %v\n", err)
		return err
	}

	var fileshares []*pb.FileShare
	for _, fs := range res {
		/*
		var tags []*pb.Tag
		for _, tag := range fs.Tags {
			tags = append(tags, &pb.Tag{
				Key:   tag.Key,
				Value: tag.Value,
			})
		}
		metadata, err := driverutils.ConvertMapToStruct(fs.Metadata)
		if err != nil {
			log.Error(err)
			return err
		}
		*/
		fileshare :=  &pb.FileShare{
			Id:                 fs.Id.Hex(),
			CreatedAt:          fs.CreatedAt,
			UpdatedAt:          fs.UpdatedAt,
			Name:               fs.Name,
			Description:        fs.Description,
			TenantId:           fs.TenantId,
			UserId:             fs.UserId,
			BackendId:          fs.BackendId,
			Backend:            fs.Backend,
			Size:               *fs.Size / utils.GB_FACTOR,
			Type:               fs.Type,
			Status:             fs.Status,
			Region:             fs.Region,
			AvailabilityZone:   fs.AvailabilityZone,
			Protocols:          fs.Protocols,
			SnapshotId:         fs.SnapshotId,
			Encrypted:          *fs.Encrypted,
			EncryptionSettings: fs.EncryptionSettings,
		}

		if f.UpdateFileShareStruct(fs, fileshare) != nil {
			log.Errorf("Failed to update fileshare struct: %v\n", fs, err)
			return err
		}

		fileshares = append(fileshares,fileshare)
	}
	out.Fileshares = fileshares
	out.Next = in.Offset + int32(len(res))

	log.Infof("List File Share successfully, #num=%d, fileshares: %+v\n", len(fileshares), fileshares)
	return nil
}

func (f *fileService) GetFileShare(ctx context.Context, in *pb.GetFileShareRequest, out *pb.GetFileShareResponse) error {
	log.Info("Received GetFileShare request.")

	fs, err := db.DbAdapter.GetFileShare(ctx, in.Id)
	if err != nil {
		log.Errorf("failed to get fileshare: %v\n", err)
		return err
	}

	/*
	var tags []*pb.Tag
	for _, tag := range fs.Tags {
		tags = append(tags, &pb.Tag{
			Key:   tag.Key,
			Value: tag.Value,
		})
	}

	metadata, err := driverutils.ConvertMapToStruct(fs.Metadata)
	if err != nil {
		log.Error(err)
		return err
	}
	*/
	fileshare :=  &pb.FileShare{
		Id:                 fs.Id.Hex(),
		CreatedAt:          fs.CreatedAt,
		UpdatedAt:          fs.UpdatedAt,
		Name:               fs.Name,
		Description:        fs.Description,
		TenantId:           fs.TenantId,
		UserId:             fs.UserId,
		BackendId:          fs.BackendId,
		Backend:            fs.Backend,
		Size:               *fs.Size / utils.GB_FACTOR,
		Type:               fs.Type,
		Status:             fs.Status,
		Region:             fs.Region,
		AvailabilityZone:   fs.AvailabilityZone,
		Protocols:          fs.Protocols,
		SnapshotId:         fs.SnapshotId,
		Encrypted:          *fs.Encrypted,
		EncryptionSettings: fs.EncryptionSettings,
	}

	if f.UpdateFileShareStruct(fs, fileshare) != nil {
		log.Errorf("Failed to update fileshare struct: %v\n", fs, err)
		return err
	}
	log.Debugf("Get File share response, fileshare: %+v\n", fileshare)

	out.Fileshare = fileshare

	log.Info("Get file share successfully.")
	return nil
}

func (f *fileService) CreateFileShare(ctx context.Context, in *pb.CreateFileShareRequest, out *pb.CreateFileShareResponse) error {
	log.Info("Received CreateFileShare request.")

	backend, err := utils.GetBackend(ctx, f.backendClient, in.Fileshare.BackendId)
	if err != nil {
		log.Errorln("failed to get backend client with err:", err)
		return err
	}

	sd, err := driver.CreateStorageDriver(backend.Backend)
	if err != nil {
		log.Errorln("Failed to create Storage driver err:", err)
		return err
	}

	fs, err := sd.CreateFileShare(ctx, in)
	if err != nil {
		log.Errorf("Received error in creating file shares at backend ", err)
		fs.Fileshare.Status = utils.FileShareStateError
	} else {
		fs.Fileshare.Status = utils.FileShareStateCreating
	}
/*
	var tags []model.Tag
	for _, tag := range in.Fileshare.Tags {
		tags = append(tags, model.Tag{
			Key:   tag.Key,
			Value: tag.Value,
		})
	}

	listFields = make(map[string]*pstruct.Value)

	fields := fs.Fileshare.Metadata.GetFields()

	metadata, err := ParseStructFields(fields)
	if err != nil {
		log.Errorf("Failed to get metadata: %v", err)
		return err
	}
	if len(listFields) != 0 {
		meta, err := ParseStructFields(listFields)
		if err != nil {
			log.Errorf("Failed to get array for metadata : %v", err)
			return err
		}
		for k, v := range meta {
			metadata[k] = v
		}
	}
*/
	fileshare := &model.FileShare{
		Name:               in.Fileshare.Name,
		Description:        in.Fileshare.Description,
		TenantId:           in.Fileshare.TenantId,
		UserId:             in.Fileshare.UserId,
		BackendId:          in.Fileshare.BackendId,
		Backend:            backend.Backend.Name,
		CreatedAt:          time.Now().Format(utils.TimeFormat),
		UpdatedAt:          time.Now().Format(utils.TimeFormat),
		Type:               in.Fileshare.Type,
		Status:             fs.Fileshare.Status,
		Region:             in.Fileshare.Region,
		AvailabilityZone:   in.Fileshare.AvailabilityZone,
		Protocols:          in.Fileshare.Protocols,
		SnapshotId:         in.Fileshare.SnapshotId,
		Size:               &fs.Fileshare.Size,
		Encrypted:          &fs.Fileshare.Encrypted,
		EncryptionSettings: fs.Fileshare.EncryptionSettings,
	}

	if f.UpdateFileShareModel(fs.Fileshare, fileshare) != nil {
		log.Errorf("Failed to update fileshare model: %v\n", fileshare, err)
		return err
	}

	if in.Fileshare.Id != "" {
		fileshare.Id = bson.ObjectIdHex(in.Fileshare.Id)
	}
	log.Debugf("Create File Share Model: %+v", fileshare)

	res, err := db.DbAdapter.CreateFileShare(ctx, fileshare)
	if err != nil {
		log.Errorf("Failed to create file share: %v", err)
		return err
	}

	metadataFS, err := driverutils.ConvertMapToStruct(fileshare.Metadata)
	if err != nil {
		log.Error(err)
		return err
	}

	out.Fileshare = &pb.FileShare{
		Id:                 res.Id.Hex(),
		CreatedAt:          res.CreatedAt,
		UpdatedAt:          res.UpdatedAt,
		Name:               res.Name,
		Description:        res.Description,
		TenantId:           res.TenantId,
		UserId:             res.UserId,
		BackendId:          res.BackendId,
		Backend:            res.Backend,
		Size:               *res.Size / utils.GB_FACTOR,
		Type:               res.Type,
		Status:             res.Status,
		Region:             res.Region,
		AvailabilityZone:   res.AvailabilityZone,
		Tags:               in.Fileshare.Tags,
		Protocols:          res.Protocols,
		SnapshotId:         res.SnapshotId,
		Encrypted:          *res.Encrypted,
		EncryptionSettings: res.EncryptionSettings,
		Metadata:           metadataFS,
	}
	/*
		//time.Sleep(4 * time.Second)
		fsInput := &pb.GetFileShareRequest{Fileshare:out.Fileshare}

		getFs, err := sd.GetFileShare(ctx, fsInput)
		if err != nil {
			log.Errorf("Received error in getting file shares at backend ", err)
			return err
		}

		log.Infof("Get File share response = [%+v]", getFs)

		listFields = make(map[string]*pstruct.Value)

		fields = getFs.Fileshare.Metadata.GetFields()

		metadata, err = ParseStructFields(fields)
		if err != nil {
			log.Errorf("Failed to get metadata: %v", err)
			return err
		}

		if len(listFields) != 0 {
			meta, err := ParseStructFields(listFields)
			if err != nil {
				log.Errorf("Failed to get array for metadata : %v", err)
				return err
			}
			for k, v := range meta {
				metadata[k] = v
			}
		}

		log.Infof("For Update FS Model Metadata: %+v", metadata)

		fileshare.Id = res.Id
		fileshare.Status = getFs.Fileshare.Status
		fileshare.Size = &getFs.Fileshare.Size
		fileshare.Encrypted = &getFs.Fileshare.Encrypted
		fileshare.EncryptionSettings = getFs.Fileshare.EncryptionSettings
		fileshare.Metadata = metadata
		fileshare.UpdatedAt = time.Now().Format(utils.TimeFormat)

		log.Infof("For Update FS Model: %+v", fileshare)

		res, err = db.DbAdapter.UpdateFileShare(ctx, fileshare)
		if err != nil {
			log.Errorf("Failed to update file share: %v", err)
			return err
		}
	*/
	log.Info("Create file share successfully.")
	return nil
}

func (f *fileService) PullFileShare(ctx context.Context, in *pb.GetFileShareRequest,
	out *pb.GetFileShareResponse) error {

	log.Info("Received PullFileShare request.")

	res, err := db.DbAdapter.GetFileShare(ctx, in.Id)
	if err != nil {
		log.Errorf("failed to get fileshare: [%v] from db\n", res, err)
		return err
	}

	metadataFS, err := driverutils.ConvertMapToStruct(res.Metadata)
	if err != nil {
		log.Error(err)
		return err
	}

	out.Fileshare = &pb.FileShare{
		Id:                 res.Id.Hex(),
		CreatedAt:          res.CreatedAt,
		UpdatedAt:          res.UpdatedAt,
		Name:               res.Name,
		Description:        res.Description,
		TenantId:           res.TenantId,
		UserId:             res.UserId,
		BackendId:          res.BackendId,
		Backend:            res.Backend,
		Size:               *res.Size / utils.GB_FACTOR,
		Type:               res.Type,
		Status:             res.Status,
		Region:             res.Region,
		AvailabilityZone:   res.AvailabilityZone,
		Protocols:          res.Protocols,
		SnapshotId:         res.SnapshotId,
		Encrypted:          *res.Encrypted,
		EncryptionSettings: res.EncryptionSettings,
		Metadata:           metadataFS,
	}

	backend, err := utils.GetBackend(ctx, f.backendClient, out.Fileshare.BackendId)
	if err != nil {
		log.Errorln("failed to get backend client with err:", err)
		return err
	}

	sd, err := driver.CreateStorageDriver(backend.Backend)
	if err != nil {
		log.Errorln("Failed to create Storage driver err:", err)
		return err
	}

	fsInput := &pb.GetFileShareRequest{Fileshare:out.Fileshare}

	getFs, err := sd.GetFileShare(ctx, fsInput)
	if err != nil {
		log.Errorf("Received error in getting file shares at backend ", err)
		return err
	}

	log.Debugf("Get File share response= [%+v] from backend", getFs)
/*
	listFields = make(map[string]*pstruct.Value)

	fields = getFs.Fileshare.Metadata.GetFields()

	metadata, err = ParseStructFields(fields)
	if err != nil {
		log.Errorf("Failed to get metadata: %v", err)
		return err
	}

	if len(listFields) != 0 {
		meta, err := ParseStructFields(listFields)
		if err != nil {
			log.Errorf("Failed to get array for metadata : %v", err)
			return err
		}
		for k, v := range meta {
			metadata[k] = v
		}
	}
   log.Infof("For Update FS Model Metadata: %+v", metadata)
*/
	if f.UpdateFileShareModel(getFs.Fileshare, res) != nil {
		log.Errorf("Failed to update file share model: %v\n", res, err)
		return err
	}

	res.Status = getFs.Fileshare.Status
	res.Size = &getFs.Fileshare.Size
	res.Encrypted = &getFs.Fileshare.Encrypted
	res.EncryptionSettings = getFs.Fileshare.EncryptionSettings
	res.UpdatedAt = time.Now().Format(utils.TimeFormat)

	log.Infof("For Update FS Model: %+v", res)

	res, err = db.DbAdapter.UpdateFileShare(ctx, res)
	if err != nil {
		log.Errorf("Failed to update file share: %v", err)
		return err
	}

	fs, err := db.DbAdapter.GetFileShare(ctx, in.Id)
	if err != nil {
		log.Errorf("failed to get fileshare: %v\n", err)
		return err
	}

	fileshare :=  &pb.FileShare{
		Id:                 fs.Id.Hex(),
		CreatedAt:          fs.CreatedAt,
		UpdatedAt:          fs.UpdatedAt,
		Name:               fs.Name,
		Description:        fs.Description,
		TenantId:           fs.TenantId,
		UserId:             fs.UserId,
		BackendId:          fs.BackendId,
		Backend:            fs.Backend,
		Size:               *fs.Size / utils.GB_FACTOR,
		Type:               fs.Type,
		Status:             fs.Status,
		Region:             fs.Region,
		AvailabilityZone:   fs.AvailabilityZone,
		Protocols:          fs.Protocols,
		SnapshotId:         fs.SnapshotId,
		Encrypted:          *fs.Encrypted,
		EncryptionSettings: fs.EncryptionSettings,
	}

	if f.UpdateFileShareStruct(fs, fileshare) != nil {
		log.Errorf("Failed to update fileshare struct: %v\n", fs, err)
		return err
	}
	log.Debugf("Get File share response, fileshare: %+v\n", fileshare)

	out.Fileshare = fileshare

	log.Info("Get file share successfully.")
	return nil
}

func (f *fileService) PullAllFileShare(ctx context.Context, in *pb.ListFileShareRequest,
	out *pb.ListFileShareResponse) error {

	log.Info("Received PullAllFileShare request.")

	backend, err := utils.GetBackend(ctx, f.backendClient, in.Filter[common.REQUEST_PATH_BACKEND_ID])
	if err != nil {
		log.Errorln("failed to get backend client with err:", err)
		return err
	}

	sd, err := driver.CreateStorageDriver(backend.Backend)
	if err != nil {
		log.Errorln("Failed to create Storage driver err:", err)
		return err
	}

	listFs, err := sd.ListFileShare(ctx, in)
	if err != nil {
		log.Errorf("Received error in getting file shares at backend ", err)
		return err
	}

	log.Debugf("Get File share response= [%+v] from backend", listFs)

	if f.ListFileShare(ctx, in, out) != nil {
		log.Errorf("Failed to List FileShares: %v\n", err)
		return err
	}

	m := make(map[string]*pb.FileShare)
	for _, fs := range listFs.Fileshares {
		m[fs.Name] = fs
	}

	for _, fs := range out.Fileshares {

		fsBackend := m[fs.Name]

		if fsBackend == nil {
			log.Errorf("Failed to retieve File share for Name = %s", fs.Name)
			return err
		}

		fs.Status = fsBackend.Status
		fs.Size = fsBackend.Size
		fs.Encrypted = fsBackend.Encrypted
		fs.EncryptionSettings = fsBackend.EncryptionSettings
		fs.Tags = fsBackend.Tags
		fs.Metadata = fsBackend.Metadata
		fs.UpdatedAt = time.Now().Format(utils.TimeFormat)

		fileshare := &model.FileShare{
			Id:                 bson.ObjectIdHex(fs.Id),
			Name:               fs.Name,
			Description:        fs.Description,
			TenantId:           fs.TenantId,
			UserId:             fs.UserId,
			BackendId:          fs.BackendId,
			Backend:            fs.Backend,
			CreatedAt:          fs.CreatedAt,
			UpdatedAt:          fs.UpdatedAt,
			Type:               fs.Type,
			Status:             fs.Status,
			Region:             fs.Region,
			AvailabilityZone:   fs.AvailabilityZone,
			Protocols:          fs.Protocols,
			SnapshotId:         fs.SnapshotId,
			Size:               &fs.Size,
			Encrypted:          &fs.Encrypted,
			EncryptionSettings: fs.EncryptionSettings,
		}

		if f.UpdateFileShareModel(fs, fileshare) != nil {
			log.Errorf("Failed to update fileshare model: %v\n", fileshare, err)
			return err
		}

		log.Debugf("For Update FS Model: %+v", fileshare)

		_, err = db.DbAdapter.UpdateFileShare(ctx, fileshare)
		if err != nil {
			log.Errorf("Failed to update file share: %v", err)
			return err
		}
	}

	res, err := db.DbAdapter.ListFileShare(ctx, int(in.Limit), int(in.Offset), in.Filter)
	if err != nil {
		log.Errorf("Failed to List FileShares: %v\n", err)
		return err
	}

	var fileshares []*pb.FileShare
	for _, fs := range res {
		fileshare :=  &pb.FileShare{
			Id:                 fs.Id.Hex(),
			CreatedAt:          fs.CreatedAt,
			UpdatedAt:          fs.UpdatedAt,
			Name:               fs.Name,
			Description:        fs.Description,
			TenantId:           fs.TenantId,
			UserId:             fs.UserId,
			BackendId:          fs.BackendId,
			Backend:            fs.Backend,
			Size:               *fs.Size / utils.GB_FACTOR,
			Type:               fs.Type,
			Status:             fs.Status,
			Region:             fs.Region,
			AvailabilityZone:   fs.AvailabilityZone,
			Protocols:          fs.Protocols,
			SnapshotId:         fs.SnapshotId,
			Encrypted:          *fs.Encrypted,
			EncryptionSettings: fs.EncryptionSettings,
		}

		if f.UpdateFileShareStruct(fs, fileshare) != nil {
			log.Errorf("Failed to update fileshare struct: %v\n", fs, err)
			return err
		}

		fileshares = append(fileshares,fileshare)
	}
	out.Fileshares = fileshares
	out.Next = in.Offset + int32(len(res))

	log.Infof("List File Share successfully, #num=%d, fileshares: %+v\n", len(fileshares), fileshares)
	return nil
}
