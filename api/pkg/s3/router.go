package s3

import (
	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/client"
)


//RegisterRouter - route request to appropriate method
func RegisterRouter(ws *restful.WebService) {
	handler := NewAPIService(client.DefaultClient)
	ws.Path("/s3")

	ws.Route(ws.PUT("/s3/{bucketName}").To(handler.BucketPut)).Doc("Create bucket for the user")
	//ws.Route(ws.HEAD("/s3/{bucketName}").To(handler.BucketHead)).Doc("Determine if bucket exists and if user has permission to access it")
	ws.Route(ws.GET("/s3/{bucketName}").To(handler.BucketGet)).Doc("Return list of objects in bucket")
	ws.Route(ws.DELETE("/s3/{bucketName}").To(handler.BucketDelete)).Doc("Delete bucket")

}
