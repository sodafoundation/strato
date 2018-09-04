package dataflow

import (
	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/client"
)

func RegisterRouter(ws *restful.WebService) {
	handler := NewAPIService(client.DefaultClient)
	ws.Route(ws.GET("/policies/{name}").To(handler.GetPolicy)).
		Doc("Get policy details")
	ws.Route(ws.POST("/policies/{name}").To(handler.CreatePolicy)).
		Doc("Create policy details")
}
