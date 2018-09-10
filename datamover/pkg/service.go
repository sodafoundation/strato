package pkg

import (
	"context"

	"github.com/micro/go-log"
	pb "github.com/opensds/go-panda/datamover/proto"
)

type DatamoverService struct{}

func (b *DatamoverService) Runjob(ctx context.Context, in *pb.RunJobRequest, out *pb.RunJobResponse) error {
	log.Log("Runjob is called in datamover service.")
	log.Logf("Request: %+v\n", in)
	out.Err = "success"
	return nil
}

func NewDatamoverService() pb.DatamoverHandler {
	return &DatamoverService{}
}
