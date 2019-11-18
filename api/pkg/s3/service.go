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

	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/client"
	"github.com/opensds/multi-cloud/backend/proto"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	. "github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

const (
	s3Service      = "s3"
	backendService = "backend"
)

type APIService struct {
	s3Client      s3.S3Service
	backendClient backend.BackendService
}

func NewAPIService(c client.Client) *APIService {
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

func (s *APIService) getBucketMeta(ctx context.Context, bucketName string) *s3.Bucket {
	rsp, err := s.s3Client.GetBucket(ctx, &s3.Bucket{Name: bucketName})
	if err != nil || rsp.ErrorCode != int32(ErrNoErr) {
		log.Infof("get bucket[name=%s] failed, err=%v, rsp.ErrorCode=%d\n", bucketName, err, rsp.ErrorCode)
		return nil
	}

	return rsp.BucketMeta
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
		log.Infof("backendRep=%+v\n", backendRep)
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
