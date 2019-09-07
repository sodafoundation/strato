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

package service

import (
	"context"
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/opensds/multi-cloud/backend/pkg/db"
	"github.com/opensds/multi-cloud/backend/pkg/model"
	"github.com/opensds/multi-cloud/backend/pkg/utils/constants"
	pb "github.com/opensds/multi-cloud/backend/proto"
)

type backendService struct{}

func NewBackendService() pb.BackendHandler {
	return &backendService{}
}

func (b *backendService) CreateBackend(ctx context.Context, in *pb.CreateBackendRequest, out *pb.CreateBackendResponse) error {
	log.Info("Received CreateBackend request.")
	backend := &model.Backend{
		Name:       in.Backend.Name,
		TenantId:   in.Backend.TenantId,
		UserId:     in.Backend.UserId,
		Type:       in.Backend.Type,
		Region:     in.Backend.Region,
		Endpoint:   in.Backend.Endpoint,
		BucketName: in.Backend.BucketName,
		Access:     in.Backend.Access,
		Security:   in.Backend.Security,
	}
	res, err := db.Repo.CreateBackend(backend)
	if err != nil {
		log.Errorf("Failed to create backend: %v", err)
		return err
	}
	out.Backend = &pb.BackendDetail{
		Id:         res.Id.Hex(),
		Name:       res.Name,
		TenantId:   res.TenantId,
		UserId:     res.UserId,
		Type:       res.Type,
		Region:     res.Region,
		Endpoint:   res.Endpoint,
		BucketName: res.BucketName,
		Access:     res.Access,
		Security:   res.Security,
	}
	log.Info("Create backend successfully.")
	return nil

}

func (b *backendService) GetBackend(ctx context.Context, in *pb.GetBackendRequest, out *pb.GetBackendResponse) error {
	log.Info("Received GetBackend request.")
	res, err := db.Repo.GetBackend(in.Id)
	if err != nil {
		log.Errorf("failed to get backend: %v\n", err)
		return err
	}
	out.Backend = &pb.BackendDetail{
		Id:         res.Id.Hex(),
		Name:       res.Name,
		TenantId:   res.TenantId,
		UserId:     res.UserId,
		Type:       res.Type,
		Region:     res.Region,
		Endpoint:   res.Endpoint,
		BucketName: res.BucketName,
		Access:     res.Access,
		Security:   res.Security,
	}
	log.Info("Get backend successfully.")
	return nil
}

func (b *backendService) ListBackend(ctx context.Context, in *pb.ListBackendRequest, out *pb.ListBackendResponse) error {
	log.Info("Received ListBackend request.")
	// (query *model.QueryField, sort *model.SortField, sortBy *model.SortBy, page *model.Pagination

	if in.Limit < 0 || in.Offset < 0 {
		msg := fmt.Sprintf("invalid pagination parameter, limit = %d and offset = %d.", in.Limit, in.Offset)
		log.Info(msg)
		return errors.New(msg)
	}

	res, err := db.Repo.ListBackend(int(in.Limit), int(in.Offset), in.Filter)
	if err != nil {
		log.Errorf("failed to list backend: %v\n", err)
		return err
	}

	var backends []*pb.BackendDetail
	for _, item := range res {
		backends = append(backends, &pb.BackendDetail{
			Id:         item.Id.Hex(),
			Name:       item.Name,
			TenantId:   item.TenantId,
			UserId:     item.UserId,
			Type:       item.Type,
			Region:     item.Region,
			Endpoint:   item.Endpoint,
			BucketName: item.BucketName,
			Access:     item.Access,
			Security:   item.Security,
		})
	}
	out.Backends = backends
	out.Next = in.Offset + int32(len(res))

	log.Info("Get backend successfully.")
	return nil
}

func (b *backendService) UpdateBackend(ctx context.Context, in *pb.UpdateBackendRequest, out *pb.UpdateBackendResponse) error {
	log.Info("Received UpdateBackend request.")
	backend, err := db.Repo.GetBackend(in.Id)
	if err != nil {
		log.Errorf("failed to get backend: %v\n", err)
		return err
	}

	// TODO: check if access and security is valid.
	backend.Access = in.Access
	backend.Security = in.Security
	res, err := db.Repo.UpdateBackend(backend)
	if err != nil {
		log.Errorf("failed to update backend: %v\n", err)
		return err
	}

	out.Backend = &pb.BackendDetail{
		Id:         res.Id.Hex(),
		Name:       res.Name,
		TenantId:   res.TenantId,
		UserId:     res.UserId,
		Type:       res.Type,
		Region:     res.Region,
		Endpoint:   res.Endpoint,
		BucketName: res.BucketName,
		Access:     res.Access,
		Security:   res.Security,
	}
	log.Info("Update backend successfully.")
	return nil
}

func (b *backendService) DeleteBackend(ctx context.Context, in *pb.DeleteBackendRequest, out *pb.DeleteBackendResponse) error {
	log.Info("Received DeleteBackend request.")
	err := db.Repo.DeleteBackend(in.Id)
	if err != nil {
		log.Errorf("failed to delete backend: %v\n", err)
		return err
	}
	log.Info("Delete backend successfully.")
	return nil
}

func (b *backendService) ListType(ctx context.Context, in *pb.ListTypeRequest, out *pb.ListTypeResponse) error {
	log.Info("Received ListType request.")
	allTypes := []*pb.TypeDetail{
		{
			Name:        constants.BackendTypeAws,
			Description: "AWS Simple Cloud Storage Service(S3)",
		},
		{
			Name:        constants.BackendTypeObs,
			Description: "Huawei Object Storage Service(OBS)",
		},
		{
			Name:        constants.BackendTypeAzure,
			Description: "Azure Blob Storage",
		},
		{
			Name:        constants.BackendTypeCeph,
			Description: "Ceph Object Storage",
		},
		{
			Name:        constants.BackendTypeGcp,
			Description: "GCP Storage",
		},
		{
			Name:        constants.BackendFusionStorage,
			Description: "Huawei Fusionstorage Object Storage",
		},
		{
			Name:        constants.BackendTypeIBMCos,
			Description: "IBM Cloud Object Storage",
		},
	}

	// Filter by name
	var types []*pb.TypeDetail
	if name, ok := in.Filter["name"]; ok {
		for _, t := range allTypes {
			if t.Name == name {
				types = append(types, t)
			}
		}
	} else {
		types = allTypes
	}

	// Pagination handle
	if int(in.Offset) > len(types) {
		return fmt.Errorf("offset exceeds the max type length")
	}
	start := int(in.Offset)
	end := start + int(in.Limit)
	if end > len(types) {
		end = len(types)
	}

	out.Types = types[start:end]
	out.Next = in.Offset + int32(len(out.Types))
	return nil
}
