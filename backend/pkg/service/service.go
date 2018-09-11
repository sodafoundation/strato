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
	out.Backend.Id = "c506cd4b-9048-43bc-97ef-0d7dec369b42"
	out.Backend.Name = "GetBackend." + in.Id
	return nil
}

func (b *backendService) ListBackend(ctx context.Context, in *pb.ListBackendRequest, out *pb.ListBackendResponse) error {
	log.Log("Received ListBackend request.")
	return nil
}

func (b *backendService) UpdateBackend(ctx context.Context, in *pb.UpdateBackendRequest, out *pb.UpdateBackendResponse) error {
	log.Log("Received UpdateBackend request.")
	return nil
}

func (b *backendService) DeleteBackend(ctx context.Context, in *pb.DeleteBackendRequest, out *pb.DeleteBackendResponse) error {
	log.Log("Received DeleteBackend request.")
	return nil
}
