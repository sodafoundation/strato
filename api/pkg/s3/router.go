// Copyright (c) 2018 Huawei Technologies Co., Ltd. All Rights Reserved.
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
	ws.Route(ws.GET("/s3?bucket").To(handler.ListBuckets)).Doc("Return list of buckets for the user")
	ws.Route(ws.PUT("/s3/{bucketName}").To(handler.RouteBucketPut)).Doc("Create bucket for the user")
	//ws.Route(ws.HEAD("/s3/{bucketName}").To(handler.BucketHead)).Doc("Determine if bucket exists and if user has permission to access it")
	ws.Route(ws.GET("/s3/{bucketName}").To(handler.BucketGet)).Doc("Return list of objects in bucket")
	ws.Route(ws.DELETE("/s3/{bucketName}").To(handler.BucketDelete)).Doc("Delete bucket")
	ws.Route(ws.PUT("/s3/{bucketName}/{objectKey}").To(handler.RouteObjectPut)).Doc("Upload object")
	ws.Route(ws.DELETE("/s3/{bucketName}/{objectKey}").To(handler.ObjectDelete)).Doc("Delete object")
	ws.Route(ws.GET("/s3/{bucketName}/{objectKey}").To(handler.ObjectGet)).Doc("Download object")
  
	ws.Route(ws.PUT("/s3/{bucketName}/{objectKey}").To(handler.RouteObjectPut)).Doc("InitMultiPartUpload")
	ws.Route(ws.PUT("/s3/{bucketName}/{objectKey}").To(handler.RouteObjectPut)).Doc("UploadPart")
	ws.Route(ws.PUT("/s3/{bucketName}/{objectKey}").To(handler.RouteObjectPut)).Doc("CompleteMultipartUpload")
	ws.Route(ws.DELETE("/s3/{bucketName}/{objectKey}").To(handler.RouteObjectPut)).Doc("AbortMultipartUpload")

}
