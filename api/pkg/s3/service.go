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
	"context"
	"io"
	"io/ioutil"
	"math"
	"os"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/v2/client"
	"github.com/opensds/multi-cloud/backend/proto"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	. "github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

const (
	MICRO_ENVIRONMENT     = "MICRO_ENVIRONMENT"
	K8S                   = "k8s"
	s3Service_Docker      = "s3"
	backendService_Docker = "backend"
	s3Service_K8S         = "soda.multicloud.v1.s3"
	backendService_K8S    = "soda.multicloud.v1.backend"
)

type APIService struct {
	s3Client      s3.S3Service
	backendClient backend.BackendService
}

func NewAPIService(c client.Client) *APIService {

	s3Service := s3Service_Docker
	backendService := backendService_Docker

	if os.Getenv(MICRO_ENVIRONMENT) == K8S {
		backendService = backendService_K8S
		s3Service = s3Service_K8S
	}

	return &APIService{
		s3Client:      s3.NewS3Service(s3Service, c),
		backendClient: backend.NewBackendService(backendService, c),
	}
}

func IsQuery(request *restful.Request, name string) bool {
	params := request.Request.URL.Query()
	if params == nil {
		return false
	}
	if _, ok := params[name]; ok {
		return true
	}
	return false
}
func HasHeader(request *restful.Request, name string) bool {
	param := request.HeaderParameter(name)
	if param == "" {
		return false
	}
	return true
}

func ReadBody(r *restful.Request) []byte {
	var reader io.Reader = r.Request.Body
	b, e := ioutil.ReadAll(reader)
	if e != nil {
		return nil
	}
	return b
}

func (s *APIService) getBucketMeta(ctx context.Context, bucketName string) (*s3.Bucket, error) {
	rsp, err := s.s3Client.GetBucket(ctx, &s3.Bucket{Name: bucketName})
	// according to gRPC framework work mechanism, if gRPC return error, then no response package can be received by
	// gRPC client, so in our codes, gRPC server will return nil and set error code to reponse package while business
	// error happens, and if gRPC client received error, that means some exception happened for gRPC itself.
	if err == nil {
		if rsp.GetErrorCode() != int32(ErrNoErr) {
			err = S3ErrorCode(rsp.GetErrorCode())
		}
	}
	if err != nil {
		log.Infof("get bucket meta data[bucket=%s] failed, err=%v\n", bucketName, err)
		return nil, err
	}

	return rsp.BucketMeta, nil
}

// if isHeadReq is true, will return expiration time and the ruleId which cause the expiration, that is need for HeadObject
func (s *APIService) getObjectMeta(ctx context.Context, bucketName, objectName, versiongId string,
	isHeadReq bool) (*s3.Object, int64, string, error) {
	rsp, err := s.s3Client.GetObjectMeta(ctx, &s3.GetObjectMetaRequest{
		BucketName: bucketName, ObjectKey: objectName, VersionId: versiongId, IsHeadReq: isHeadReq})
	// according to gRPC framework work mechanism, if gRPC return error, then no response package can be received by
	// gRPC client, so in our codes, gRPC server will return nil and set error code to reponse package while business
	// error happens, and if gRPC client received error, that means some exception happened for gRPC itself.
	if err == nil {
		if rsp.GetErrorCode() != int32(ErrNoErr) {
			err = S3ErrorCode(rsp.GetErrorCode())
		}
	}
	if err != nil {
		log.Infof("get object meta data[bucket=%s,key=%s] failed, err=%v\n", bucketName, objectName, err)
		return nil, 0, "", err
	}

	return rsp.Object, rsp.ExpireTime, rsp.RuleId, nil
}

func (s *APIService) isBackendExist(ctx context.Context, backendName string) bool {
	flag := false

	backendRep, backendErr := s.backendClient.ListBackend(ctx, &backendpb.ListBackendRequest{
		Offset: 0,
		Limit:  math.MaxInt32,
		Filter: map[string]string{"name": backendName}})
	log.Infof("backendErr is %v:", backendErr)
	if backendErr != nil {
		log.Infof("Get backend[name=%s] failed.", backendName)
	} else {
		if len(backendRep.Backends) > 0 {
			log.Infof("backend[name=%s] exist.", backendName)
			flag = true
		}
	}

	return flag
}

func HandleS3Error(response *restful.Response, request *restful.Request, err error, errCode int32) error {
	if err != nil {
		WriteErrorResponse(response, request, err)
		return err
	}
	if errCode != int32(ErrNoErr) {
		err := S3ErrorCode(errCode)
		WriteErrorResponse(response, request, err)
		return err
	}

	return nil
}
