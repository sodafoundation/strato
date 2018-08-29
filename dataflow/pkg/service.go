package pkg

import (
	"context"

	"github.com/micro/go-log"
	pb "github.com/opensds/go-panda/dataflow/proto"
)

type dataflowService struct{}

func (b *dataflowService) GetPolicy(ctx context.Context, in *pb.GetPolicyRequest, out *pb.GetPolicyResponse) error {
	log.Log("Getdataflow is called in dataflow service.")
	out.Id = "c506cd4b-9048-43bc-97ef-0d7dec369b42"
	out.Name = "GetPolicy." + in.Id
	return nil
}

func NewDataFlowService() pb.DataFlowHandler {
	return &dataflowService{}
}
