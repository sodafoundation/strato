package service

import (
	"context"

	"github.com/micro/go-log"
	"github.com/opensds/go-panda/backend/pkg/db"
	"github.com/opensds/go-panda/backend/pkg/model"
	pb "github.com/opensds/go-panda/backend/proto"
)

type backendService struct{}

func NewBackendService() pb.BackendHandler {
	return &backendService{}
}

func (b *backendService) CreateBackend(ctx context.Context, in *pb.CreateBackendRequest, out *pb.CreateBackendResponse) error {
	log.Log("Received CreateBackend request.")
	backend := &model.Backend{
		Name:       in.Backend.Name,
		TenantId:   in.Backend.TenantId,
		UserId:     in.Backend.UserId,
		Type:       in.Backend.Type,
		Endpoint:   in.Backend.Endpoint,
		BucketName: in.Backend.BucketName,
		Access:     in.Backend.Access,
		Security:   in.Backend.Security,
	}
	res, err := db.Repo.CreateBackend(backend)
	if err != nil {
		log.Logf("Failed to create backend: %v", err)
		return err
	}
	out.Backend = &pb.BackendDetail{
		Id:         res.Id.Hex(),
		Name:       res.Name,
		TenantId:   res.TenantId,
		UserId:     res.UserId,
		Type:       res.Type,
		Endpoint:   res.Endpoint,
		BucketName: res.BucketName,
		Access:     res.Access,
		Security:   res.Security,
	}
	log.Log("Create backend successfully.")
	return nil

}

func (b *backendService) GetBackend(ctx context.Context, in *pb.GetBackendRequest, out *pb.GetBackendResponse) error {
	log.Log("Received GetBackend request.")
	res, err := db.Repo.GetBackend(in.Id)
	if err != nil {
		log.Logf("Failed to get backend: %v", err)
		return err
	}
	out.Backend = &pb.BackendDetail{
		Id:         res.Id.Hex(),
		Name:       res.Name,
		TenantId:   res.TenantId,
		UserId:     res.UserId,
		Type:       res.Type,
		Endpoint:   res.Endpoint,
		BucketName: res.BucketName,
		Access:     res.Access,
		Security:   res.Security,
	}
	log.Log("Get backend successfully.")
	return nil
}

func (b *backendService) ListBackend(ctx context.Context, in *pb.ListBackendRequest, out *pb.ListBackendResponse) error {
	log.Logf("Received ListBackend request: %v", in)
	res, err := db.Repo.ListBackend()
	if err != nil {
		log.Logf("Failed to list backend: %v", err)
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
			Endpoint:   item.Endpoint,
			BucketName: item.BucketName,
			Access:     item.Access,
			Security:   item.Security,
		})
	}
	out.Backends = backends
	log.Log("Get backend successfully.")
	return nil
}

func (b *backendService) UpdateBackend(ctx context.Context, in *pb.UpdateBackendRequest, out *pb.UpdateBackendResponse) error {
	log.Log("Received UpdateBackend request.")
	backend, err := db.Repo.GetBackend(in.Id)
	if err != nil {
		log.Logf("Failed to get backend: %v", err)
		return err
	}

	backend.Access = in.Access
	backend.Security = in.Security
	res, err := db.Repo.UpdateBackend(backend)
	if err != nil {
		log.Logf("Failed to update backend: %v", err)
		return err
	}

	out.Backend = &pb.BackendDetail{
		Id:         res.Id.Hex(),
		Name:       res.Name,
		TenantId:   res.TenantId,
		UserId:     res.UserId,
		Type:       res.Type,
		Endpoint:   res.Endpoint,
		BucketName: res.BucketName,
		Access:     res.Access,
		Security:   res.Security,
	}
	log.Log("Update backend successfully.")
	return nil
}

func (b *backendService) DeleteBackend(ctx context.Context, in *pb.DeleteBackendRequest, out *pb.DeleteBackendResponse) error {
	log.Log("Received DeleteBackend request.")
	err := db.Repo.DeleteBackend(in.Id)
	if err != nil {
		log.Logf("Failed to delete backend: %v", err)
		return err
	}
	log.Log("Delete backend successfully.")
	return nil
}
