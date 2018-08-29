package main

import (
	"fmt"

	micro "github.com/micro/go-micro"
	handler "github.com/opensds/go-panda/s3/pkg"
	pb "github.com/opensds/go-panda/s3/proto"
)

func main() {
	service := micro.NewService(
		micro.Name("s3"),
	)

	micro.NewService()

	service.Init()

	pb.RegisterS3Handler(service.Server(), handler.NewS3Service())
	if err := service.Run(); err != nil {
		fmt.Println(err)
	}
}
