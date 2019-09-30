// Copyright 2019 The OpenSDS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package s3

import (
	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/client"
)

//RegisterRouter - route request to appropriate method
func RegisterRouter(ws *restful.WebService) {
	handler := NewAPIService(client.DefaultClient)
	ws.Route(ws.GET("/").To(handler.ListBuckets)).Doc("Return list of buckets for the user")
	ws.Route(ws.GET("/storageClasses").To(handler.GetStorageClasses)).Doc("Return supported storage classes.")
	ws.Route(ws.PUT("/{bucketName}").To(handler.RouteBucketPut)).Doc("Create bucket for the user")
	//ws.Route(ws.HEAD("/s3/{bucketName}").To(handler.BucketHead)).Doc("Determine if bucket exists and if user has permission to access it")
	ws.Route(ws.GET("/{bucketName}").To(handler.RouteBucketGet)).Doc("Return list of objects in bucket")
	ws.Route(ws.DELETE("/{bucketName}").To(handler.RouteBucketDelete)).Doc("Delete bucket")
	ws.Route(ws.HEAD("/{bucketName}").To(handler.RouteBucketHead)).Doc("Head bucket")

	ws.Route(ws.PUT("/{bucketName}/{objectKey:*}").To(handler.RouteObjectPut)).Doc("Upload object")
	ws.Route(ws.DELETE("/{bucketName}/{objectKey:*}").To(handler.RouteObjectDelete)).Doc("Delete object")
	ws.Route(ws.GET("/{bucketName}/{objectKey:*}").To(handler.ObjectGet)).Doc("Download object")
	ws.Route(ws.PUT("/{bucketName}/{objectKey:*}").To(handler.RouteObjectPut)).Doc("InitMultiPartUpload")
	ws.Route(ws.PUT("/{bucketName}/{objectKey:*}").To(handler.RouteObjectPut)).Doc("UploadPart")
	ws.Route(ws.PUT("/{bucketName}/{objectKey:*}").To(handler.RouteObjectPut)).Doc("CompleteMultipartUpload")
	ws.Route(ws.DELETE("/{bucketName}/{objectKey:*}").To(handler.RouteObjectDelete)).Doc("AbortMultipartUpload")
	ws.Route(ws.HEAD("/{bucketName}/{objectKey:*}").To(handler.RouteObjectHead)).Doc("Head object")
	ws.Route(ws.POST("/{bucketName}/{objectKey:*}").To(handler.RouteObjectHead)).Doc("Post object")

	//Router for PUT and GET bucket lifecycle
	ws.Route(ws.PUT("/{bucketName}/?lifecycle").To(handler.RouteBucketPut)).Doc("Create lifecycle configuration for the bucket")
	ws.Route(ws.GET("/{bucketName}/?lifecycle").To(handler.RouteBucketGet)).Doc("Get lifecycle configuration from the bucket")
	ws.Route(ws.DELETE("/{bucketName}/?lifecycle").To(handler.RouteBucketDelete)).Doc("Delete lifecycle configuration from the bucket")
}
