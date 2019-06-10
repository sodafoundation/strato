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
	//"strconv"
	//"strings"

	"net/http"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"

	s3 "github.com/opensds/multi-cloud/s3/proto"
	"golang.org/x/net/context"
)

//ObjectGet -
func (s *APIService) ObjectHead(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	objectKey := request.PathParameter("objectKey")
	ctx := context.Background()

	log.Logf("received request for HEAD Object: %s", objectKey)

	objectInput := s3.GetObjectInput{Bucket: bucketName, Key: objectKey}

	objectMD, _ := s.s3Client.GetObject(ctx, &objectInput)
	log.Logf("the object metadata is: %v\n ", objectMD)

	res := objectMD.GetRestoreStatus()
	if res != nil {
		//res := objectMD.GetRestoreStatus()
		log.Logf("marker is : %v\n", res.RestoreMarker)
		log.Logf("state is : %v\n", res.RestoreState)
		//TODO check what has to be done with Restore marker value
	} else {
		log.Logf("no such object")
		response.WriteErrorString(http.StatusInternalServerError, "Specified object doesn't exist")
		return
	}
	response.WriteEntity(res)
}
