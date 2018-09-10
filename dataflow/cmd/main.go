package main

import (
	"github.com/micro/go-log"
	"github.com/micro/go-micro"
	handler "github.com/opensds/go-panda/dataflow/pkg"
	pb "github.com/opensds/go-panda/dataflow/proto"
)

func main() {
	service := micro.NewService(
		micro.Name("dataflow"),
	)

	service.Init()

	pb.RegisterDataFlowHandler(service.Server(), handler.NewDataFlowService())
	if err := service.Run(); err != nil {
		log.Log(err)
	}


}
