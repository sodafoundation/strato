package s3

import (
	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/client"
	//	"github.com/micro/go-micro/errors"
	"github.com/opensds/go-panda/s3/proto"
	"io"
	"io/ioutil"
)

const (
	s3Service = "s3"
)

type APIService struct {
	s3Client s3.S3Service
}

func NewAPIService(c client.Client) *APIService {
	return &APIService{
		s3Client: s3.NewS3Service(s3Service, c),
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
