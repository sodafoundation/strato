package pkg

import (
	"context"

	"github.com/micro/go-log"
	pb "github.com/opensds/go-panda/backend/proto"
)

type backendService struct{}

func (b *backendService) GetBackend(ctx context.Context, in *pb.GetBackendRequest, out *pb.GetBackendResponse) error {
	log.Log("GetBackend is called in backend service.")
	out.Id = "c506cd4b-9048-43bc-97ef-0d7dec369b42"
	out.Name = "GetBackend." + in.Id
	return nil
}

func NewBackendService() pb.BackendHandler {
	return &backendService{}
}
