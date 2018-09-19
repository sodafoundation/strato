package s3

import (
	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/client"
	//	"github.com/micro/go-micro/errors"
	"context"
	"github.com/micro/go-log"
	"github.com/opensds/go-panda/api/pkg/s3/datastore"
	"github.com/opensds/go-panda/backend/proto"
	backendpb "github.com/opensds/go-panda/backend/proto"
	"github.com/opensds/go-panda/s3/proto"
	"io"
	"io/ioutil"
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

func _getBackendClient(s *APIService, bucketName string) datastore.DataStoreAdapter {
	ctx := context.Background()
	buekct, err := s.s3Client.GetBucket(ctx, &s3.Bucket{Name: bucketName})
	if err != nil {
		return nil
	}
	backendRep, backendErr := s.backendClient.GetBackend(ctx, &backendpb.GetBackendRequest{Id: buekct.Backend})
	if backendErr != nil {
		log.Logf("Get backend %s failed.", buekct.Backend)
		return nil
	}

	backend := backendRep.Backend
	client := datastore.Init(backend)
	return client
}
