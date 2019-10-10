package s3

import (
	"github.com/emicklei/go-restful"
	"github.com/opensds/multi-cloud/api/pkg/policy"
)

func (s *APIService) RouteObjectDelete(request *restful.Request, response *restful.Response) {
	if !policy.Authorize(request, response, "object:delete") {
		return
	}
	if IsQuery(request, "uploadId") {
		s.AbortMultipartUpload(request, response)
	} else {
		s.ObjectDelete(request, response)
	}
}
