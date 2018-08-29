package pkg

import (
	"context"

	"github.com/micro/go-log"
	pb "github.com/opensds/go-panda/s3/proto"
)

type s3Service struct{}

func (b *s3Service) GetObject(ctx context.Context, in *pb.GetObjectRequest, out *pb.GetObjectResponse) error {
	log.Log("GetObject is called in s3 service.")
	out.Id = "c506cd4b-9048-43bc-97ef-0d7dec369b42"
	out.Name = "GetObject." + in.Id
	return nil
}

func NewS3Service() pb.S3Handler {
	return &s3Service{}
}
