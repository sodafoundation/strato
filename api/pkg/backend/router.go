package backend

import (
	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/client"
)

func RegisterRouter(ws *restful.WebService) {
	handler := NewAPIService(client.DefaultClient)
	ws.Route(ws.GET("/backends/{id}").To(handler.GetBackend)).
		Doc("Show backend details")
	ws.Route(ws.GET("/backends").To(handler.ListBackend)).
		Doc("Get backend list")
	ws.Route(ws.POST("/backends").To(handler.CreateBackend)).
		Doc("Create backend")
	ws.Route(ws.PUT("/backends/{id}").To(handler.UpdateBackend)).
		Doc("Update backend")
	ws.Route(ws.DELETE("/backends/{id}").To(handler.DeleteBackend)).
		Doc("Delete backend")
}
