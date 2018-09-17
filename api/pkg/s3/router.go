package s3

import (
	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/client"
)

//RegisterRouter - route request to appropriate method
func RegisterRouter(ws *restful.WebService) {
	handler := NewAPIService(client.DefaultClient)
	ws.Route(ws.GET("/s3").To(handler.ListBuckets)).Doc("Return list of buckets for the user")
	ws.Route(ws.PUT("/s3/{bucketName}").To(handler.RouteBucketPut)).Doc("Create bucket for the user")
	//ws.Route(ws.HEAD("/s3/{bucketName}").To(handler.BucketHead)).Doc("Determine if bucket exists and if user has permission to access it")
	ws.Route(ws.GET("/s3/{bucketName}").To(handler.BucketGet)).Doc("Return list of objects in bucket")
	ws.Route(ws.DELETE("/s3/{bucketName}").To(handler.BucketDelete)).Doc("Delete bucket")
	ws.Route(ws.PUT("/s3/{bucketName}/{objectKey}").To(handler.RouteObjectPut)).Doc("Upload object")

}
