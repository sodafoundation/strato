package main

import (
	"fmt"

	micro "github.com/micro/go-micro"
	handler "github.com/opensds/go-panda/backend/pkg"
	pb "github.com/opensds/go-panda/backend/proto"
)

func main() {
	service := micro.NewService(
		micro.Name("backend"),
	)

	micro.NewService()

	service.Init()

	pb.RegisterBackendHandler(service.Server(), handler.NewBackendService())
	if err := service.Run(); err != nil {
		fmt.Println(err)
	}
}
