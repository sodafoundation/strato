// Copyright 2021 The OpenSDS Authors.
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

package metadata

import (
	"net/http"
	"strconv"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/v2/client"
	"github.com/opensds/multi-cloud/api/pkg/common"
	mt "github.com/opensds/multi-cloud/metadata/proto"
	log "github.com/sirupsen/logrus"
)

const (
	metadataService    = "metadata"
	limitDefaultValue  = "1000"
	offsetDefaultValue = "1000"
)

type APIService struct {
	metaClient mt.MetadataService
}

func NewAPIService(c client.Client) *APIService {
	return &APIService{
		metaClient: mt.NewMetadataService(metadataService, c),
	}
}

func (s *APIService) SyncMetadata(request *restful.Request, response *restful.Response) {
	log.Info("Sync Metadata called in api service.")
	ctx := common.InitCtxWithAuthInfo(request)
	_, err := s.metaClient.SyncMetadata(ctx, &mt.SyncMetadataRequest{Id: "id"})
	if err != nil {
		log.Errorf("failed to sync metadata details: %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	response.WriteEntity("syncing successful...")
}

func (s *APIService) ListMetadata(request *restful.Request, response *restful.Response) {
	log.Infof("Received request for Listmetadata details: %s\n", request.PathParameter("id"))

	ctx := common.InitCtxWithAuthInfo(request)

	//id := request.PathParameter("id")

	listMetadataRequest, err := GetListMetaDataRequest(request)

	//* calling  the ListMetaData method from metadata manager m8s
	res, err := s.metaClient.ListMetadata(ctx, &listMetadataRequest)
	log.Info("Get metadata details res.......:.", res)
	if err != nil {
		log.Errorf("Failed to get metadata details err: \n", err)
		response.WriteError(http.StatusNotFound, err)
		return
	}

	log.Info("Get metadata details successfully.")
	response.WriteEntity(res.Buckets)
}

//* This function fetches the request parameters from the request and assigns them default values if not present.
//* It returns ListMetadataRequest for ListMetaData API call
func GetListMetaDataRequest(request *restful.Request) (listMetadataRequest mt.ListMetadataRequest, err error) {
	typeOfCloudVendor := request.PathParameter("type")
	limitStr := request.PathParameter("limit")
	backendName := request.PathParameter("backendName")
	offsetStr := request.PathParameter("offset")
	bucketName := request.PathParameter("bucketName")
	objectName := request.PathParameter("objectName")
	sizeOfObject := request.PathParameter("sizeOfObject")
	sizeOfBucket := request.PathParameter("sizeOfBucket")

	offsetStr, limitStr = assignDefaultValues(offsetStr, limitStr)

	// convert string offset to int32
	offset, err := strconv.ParseInt(offsetStr, 10, 32)
	if err != nil {
		log.Errorf("Failed to parse offset as int32: \n", err)
		return mt.ListMetadataRequest{}, err
	}

	// convert string limit to int32
	limit, err := strconv.ParseInt(limitStr, 10, 32)
	if err != nil {
		log.Errorf("Failed to parse limit as int32: \n", err)
		return mt.ListMetadataRequest{}, err
	}

	return mt.ListMetadataRequest{Type: typeOfCloudVendor, BackendName: backendName, Limit: int32(limit), Offset: int32(offset), BucketName: bucketName, ObjectName: objectName, SizeOfObject: sizeOfObject, SizeOfBucket: sizeOfBucket}, nil

}

//* This function assigns default values if values are not specified by the user
func assignDefaultValues(offsetStr string, limitStr string) (string, string) {
	if offsetStr == "" {
		offsetStr = offsetDefaultValue
	}

	if limitStr == "" {
		limitStr = limitDefaultValue
	}
	return offsetStr, limitStr
}
