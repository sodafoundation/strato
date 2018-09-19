package main

import (
	"github.com/micro/go-log"
	"github.com/micro/go-micro"
	handler "github.com/opensds/go-panda/dataflow/pkg"
	pb "github.com/opensds/go-panda/dataflow/proto"
	"github.com/opensds/go-panda/datamover/proto"
	"github.com/micro/go-micro/client"
	"github.com/opensds/go-panda/dataflow/pkg/scheduler"
	_ "github.com/opensds/go-panda/dataflow/pkg/scheduler/trigger/crontrigger"
)

func main() {
	service := micro.NewService(
		micro.Name("dataflow"),
	)

	service.Init()
	dm := datamover.NewDatamoverService("datamover", client.DefaultClient)
	pb.RegisterDataFlowHandler(service.Server(), handler.NewDataFlowService(dm))
	scheduler.LoadAllPlans(dm)
	if err := service.Run(); err != nil {
		log.Log(err)
	}
}
