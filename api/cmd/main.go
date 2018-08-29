package main

import (
	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/client"
	"github.com/micro/go-web"
	service "github.com/opensds/go-panda/api/pkg"

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
	handler := service.NewAPIService(client.DefaultClient)

	wc := restful.NewContainer()
	ws := new(restful.WebService)

	ws.
		Path("/v1").
		Doc("OpenSDS Multi-Cloud API").
		Consumes(restful.MIME_JSON).
		Produces(restful.MIME_JSON)

	ws.Route(ws.GET("/backends/{id}").To(handler.GetBackend)).
		Doc("Get backend details")

	ws.Route(ws.GET("/objects/{id}").To(handler.GetObject)).
		Doc("Get object details")

	ws.Route(ws.GET("/policies/{id}").To(handler.GetPolicy)).
		Doc("Get policy details")

	wc.Add(ws)
	webService.Handle("/", wc)
	if err := webService.Run(); err != nil {
		log.Fatal(err)
	}

}
