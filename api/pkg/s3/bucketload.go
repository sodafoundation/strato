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
	"errors"
	"net/http"

	"encoding/xml"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	"golang.org/x/net/context"
	pb "github.com/opensds/multi-cloud/s3/proto"
)

func (s *APIService) BucketLoad(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	ctx := context.Background()

	body := ReadBody(request)
	if body == nil {
		log.Logf("received load objects of bucket[%s], but body is nil\n", bucketName)
		response.WriteError(http.StatusBadRequest, errors.New("request body is nil"))
		return
	}

	loadObjsReq := model.LoadObjectsReq{}
	err := xml.Unmarshal(body, &loadObjsReq)
	if err != nil {
		log.Logf("received load objects of bucket[%s], but parse body failed, err:%v\n", bucketName, err)
		response.WriteError(http.StatusBadRequest, errors.New("request body is nil"))
	}

	log.Logf("received load objects of bucket[%s], backend=%s, prefix=%s\n", bucketName, loadObjsReq.Backend, loadObjsReq.Prefix)

	client := getBackendByName(s, loadObjsReq.Backend)
	if client == nil {
		log.Logf("get backend[name=%s] failed\n", loadObjsReq.Backend)
		response.WriteError(http.StatusInternalServerError, NoSuchBackend.Error())
		return
	}

	marker := ""
	var limit, total, succeed int64 = 1000, 0, 0
	//folders := map[string]struct{}{}
	preLen := len(bucketName + "/")
	for {
		objList, s3err := client.ListBackendObjects(ctx, loadObjsReq.Prefix, limit, marker)
		if s3err != NoError {
			log.Logf("list object of backend[name=%s] failed\n", loadObjsReq.Backend)
			response.WriteError(http.StatusInternalServerError, errors.New("list objects of backend failed"))
			return
		}

		for _, obj := range objList.ListObjects {
			total++

			obj.ObjectKey = obj.ObjectKey[preLen:]
			obj.Backend = loadObjsReq.Backend
			obj.BucketName = bucketName
			res, err := s.s3Client.GetTierByStorageClass(ctx, &pb.StorageTierAndClass{
				BackendType: client.GetBackendType(),
				StorageClass: obj.StorageClass,
			})
			if err != nil {
				continue
			}
			obj.Tier = res.Tier

			//createDirectorIfNeed(s, obj, &folders)
			_, err = s.s3Client.CreateObject(ctx, obj)
			if err != nil {
				continue
			}
			succeed++
		}

		if int64(len(objList.ListObjects)) < limit {
			break
		}
	}

	log.Logf("load objects of backend[%s], total=%d, succeed=%d\n", loadObjsReq.Backend, total, succeed)
	res := model.LoadObjectsResponse{Total: total, Succeed: succeed}
	response.WriteEntity(res)
}

/*func createDirectorIfNeed(s *APIService, obj *pb.Object, folders *map[string]struct{}) {
	// example: if objkey is 'fa/fb/cc', then
	fold := pb.Object{
		ObjectKey: obj.ObjectKey,
		StorageClass: obj.StorageClass,
		LastModified: obj.LastModified,
		BucketName: obj.BucketName,
		Backend: obj.Backend,
	}
	key := fold.ObjectKey
	subKeys := strings.Split(key, "/")
	nums := len(subKeys)
	for i := 0; i < nums - 1; i++ {
		if _, ok := (*folders)[subKeys[i]]; ok {
			// folder is already created
			continue
		}

		fold.ObjectKey = subKeys[i] + "/"
		_, err := s.s3Client.CreateObject(context.Background(), &fold)
		if err != nil {
			continue
		}
		(*folders)[subKeys[i]] = struct{}{}
	}
}*/

