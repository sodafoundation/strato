package s3

import (
	"github.com/emicklei/go-restful"

	"github.com/opensds/multi-cloud/api/pkg/filters/signature"
)

func (s *APIService) RouteObjectDelete(request *restful.Request, response *restful.Response) {
	err := signature.PayloadCheck(request, response)
	if err != nil {
		WriteErrorResponse(response, request, err)
		return
	}
	if IsQuery(request, "uploadId") {
		s.AbortMultipartUpload(request, response)
	} else {
		s.ObjectDelete(request, response)
	}
}
