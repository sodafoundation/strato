package s3

import (
	"github.com/emicklei/go-restful"
	s3error "github.com/opensds/multi-cloud/s3/error"
)

func (s *APIService) ListObjectParts(request *restful.Request, response *restful.Response) {
	WriteErrorResponse(response, request, s3error.ErrNotImplemented)
}
