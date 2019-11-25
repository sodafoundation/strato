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
	"github.com/opensds/multi-cloud/api/pkg/common"
	. "github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	s3 "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

//Function for GET Bucket Versioning API
func (s *APIService) BucketVersioningGet(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	log.Infof("received request for bucket details in GET versioning: %s", bucketName)

	ctx := common.InitCtxWithAuthInfo(request)
	versionInfo, err := s.s3Client.GetBucketVersioning(ctx, &s3.BaseBucketRequest{BucketName: bucketName})
	if err != nil {
		log.Errorf("get bucket[%s] versioning failed, err=%v\n", bucketName, err)
		return
	}

	// convert back to xml struct
	versionConfXml := model.VersioningConfiguration{}

	// convert versioning configuration to xml
	versionConfXml.Status = versionInfo.Status

	// marshall the array back to xml format
	err = response.WriteAsXml(versionConfXml)
	if err != nil {
		log.Infof("write version of bucket[%s] as xml failed, versioning =%s, err=%v.\n", bucketName,
			versionConfXml, err)
		WriteErrorResponse(response, request, ErrInternalError)
		return
	}
	log.Info("GET versioning configuration succeed.")
}
