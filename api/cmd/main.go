package main

import (
	"github.com/emicklei/go-restful"
	"github.com/micro/go-web"
	"github.com/opensds/go-panda/api/pkg/backend"
	"github.com/opensds/go-panda/api/pkg/dataflow"
	"github.com/opensds/go-panda/api/pkg/s3"

	"github.com/micro/go-log"
	//	_ "github.com/micro/go-plugins/client/grpc"
)

const (
	serviceName = "go.panda"
)

func main() {
	webService := web.NewService(
		web.Name(serviceName),
		web.Version("v0.1.0"),
	)
	webService.Init()

	wc := restful.NewContainer()
	ws := new(restful.WebService)
	ws.Path("/v1")
	ws.Doc("OpenSDS Multi-Cloud API")
	ws.Consumes(restful.MIME_JSON)
	ws.Produces(restful.MIME_JSON)

	backend.RegisterRouter(ws)
	s3.RegisterRouter(ws)
	dataflow.RegisterRouter(ws)

	wc.Add(ws)
	webService.Handle("/", wc)
	if err := webService.Run(); err != nil {
		log.Fatal(err)
	}

}
