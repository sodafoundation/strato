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
	log.Info("This is just for testing... Do not merge")
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
	res, err := db.Repo.CreateBackend(ctx, backend)
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
	res, err := db.Repo.GetBackend(ctx, in.Id)
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

	res, err := db.Repo.ListBackend(ctx, int(in.Limit), int(in.Offset), in.Filter)
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

	log.Infof("Get backend successfully, #num=%d", len(backends))
	return nil
}

func (b *backendService) UpdateBackend(ctx context.Context, in *pb.UpdateBackendRequest, out *pb.UpdateBackendResponse) error {
	log.Info("Received UpdateBackend request.")
	backend, err := db.Repo.GetBackend(ctx, in.Id)
	if err != nil {
		log.Errorf("failed to get backend: %v\n", err)
		return err
	}

	// TODO: check if access and security is valid.
	backend.Access = in.Access
	backend.Security = in.Security
	res, err := db.Repo.UpdateBackend(ctx, backend)
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
	err := db.Repo.DeleteBackend(ctx, in.Id)
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
			Name:        constants.BackendTypeGcs,
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
		{
			Name:        constants.BackendTypeYIGS3,
			Description: "YIG Storage",
		},
		{
			Name:        constants.BackendTypeAlibaba,
			Description: "Alibaba Object Storage Service(OSS)",
		},
		{
			Name:        constants.BackendTypeAwsFile,
			Description: "AWS File Service",
		},
		{
			Name:        constants.BackendTypeAzureFile,
			Description: "Azure File Service",
		},
		{
			Name:        constants.BackendTypeGcsFile,
			Description: "GCP File Service",
		},
		{
			Name:        constants.BackendTypeAwsBlock,
			Description: "AWS Block Service",
		},
		{
			Name:        constants.BackendTypeHwSFS,
			Description: "Huawei File Service",
		},
		{
			Name:        constants.BackendTypeHpcBlock,
			Description: "HPC Block Service",
		},
		{
			Name:        constants.BackendTypeSonyODA,
			Description: "Sony-ODA Object Storage",
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
	log.Infof("Types:%+v\n", out.Types)
	return nil
}

func (b *backendService) CreateTier(ctx context.Context, in *pb.CreateTierRequest, out *pb.CreateTierResponse) error {
	log.Info("Received CreateTier request.")
	tier := &model.Tier{
		Name:     in.Tier.Name,
		TenantId: in.Tier.TenantId,
		Backends: in.Tier.Backends,
		Tenants:  in.Tier.Tenants,
	}
	res, err := db.Repo.CreateTier(ctx, tier)
	if err != nil {
		log.Errorf("Failed to create tier: %v", err)
		return err
	}
	out.Tier = &pb.Tier{
		Id:       res.Id.Hex(),
		Name:     res.Name,
		TenantId: res.TenantId,
		Backends: res.Backends,
		Tenants:  in.Tier.Tenants,
	}
	log.Info("Create tier successfully.")
	return nil
}

func (b *backendService) UpdateTier(ctx context.Context, in *pb.UpdateTierRequest, out *pb.UpdateTierResponse) error {
	log.Info("Received UpdateTier request.")

	res1, err := db.Repo.GetTier(ctx, in.Tier.Id)
	if err != nil {
		log.Errorf("failed to update tier: %v\n", err)
		return err
	}

	tier := &model.Tier{
		Id:       res1.Id,
		Name:     res1.Name,
		TenantId: res1.TenantId,
		Backends: in.Tier.Backends,
		Tenants:  in.Tier.Tenants,
	}

	res, err := db.Repo.UpdateTier(ctx, tier)
	if err != nil {
		log.Errorf("failed to update tier: %v\n", err)
		return err
	}

	out.Tier = &pb.Tier{
		Id:       res.Id.Hex(),
		Name:     res.Name,
		TenantId: res.TenantId,
		Backends: res.Backends,
		Tenants:  in.Tier.Tenants,
	}
	log.Info("Update tier successfully.")
	return nil
}

func (b *backendService) GetTier(ctx context.Context, in *pb.GetTierRequest, out *pb.GetTierResponse) error {
	log.Info("Received GetTier request.")
	res, err := db.Repo.GetTier(ctx, in.Id)
	if err != nil {
		log.Errorf("failed to get tier: %v\n", err)
		return err
	}
	out.Tier = &pb.Tier{
		Id:       res.Id.Hex(),
		Name:     res.Name,
		TenantId: res.TenantId,
		Backends: res.Backends,
		Tenants:  res.Tenants,
	}
	log.Info("Get Tier successfully.")
	return nil

}

func (b *backendService) ListTiers(ctx context.Context, in *pb.ListTierRequest, out *pb.ListTierResponse) error {
	log.Info("Received ListTiers request.")
	if in.Limit < 0 || in.Offset < 0 {
		msg := fmt.Sprintf("invalid pagination parameter, limit = %d and offset = %d.", in.Limit, in.Offset)
		log.Info(msg)
		return errors.New(msg)
	}
	res, err := db.Repo.ListTiers(ctx, int(in.Limit), int(in.Offset), in.Filter)
	if err != nil {
		log.Errorf("failed to list backend: %v\n", err)
		return err
	}

	var tiers []*pb.Tier
	for _, item := range res {
		tiers = append(tiers, &pb.Tier{
			Id:       item.Id.Hex(),
			Name:     item.Name,
			TenantId: item.TenantId,
			Backends: item.Backends,
			Tenants:  item.Tenants,
		})
	}
	out.Tiers = tiers
	out.Next = in.Offset + int32(len(res))

	log.Infof("List Tiers successfully, #num=%d", len(tiers))
	return nil

}

func (b *backendService) DeleteTier(ctx context.Context, in *pb.DeleteTierRequest, out *pb.DeleteTierResponse) error {
	log.Info("Received DeleteTier request.")
	err := db.Repo.DeleteTier(ctx, in.Id)
	if err != nil {
		log.Errorf("failed to delete tier: %v\n", err)
		return err
	}
	log.Info("Delete tier successfully.")
	return nil

}
