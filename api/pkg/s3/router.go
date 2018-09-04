package s3

import (
	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/client"
)

func RegisterRouter(ws *restful.WebService) {
	handler := NewAPIService(client.DefaultClient)
	ws.Route(ws.GET("/objects/{id}").To(handler.GetObject)).
		Doc("Get object details")
}
