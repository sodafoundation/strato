package main

import (
	"fmt"

	micro "github.com/micro/go-micro"
	handler "github.com/opensds/go-panda/datamover/pkg"
	pb "github.com/opensds/go-panda/datamover/proto"
)

func main() {
	service := micro.NewService(
		micro.Name("datamover"),
	)

	service.Init()

	pb.RegisterDatamoverHandler(service.Server(), handler.NewDatamoverService())
	if err := service.Run(); err != nil {
		fmt.Println(err)
	}
}
