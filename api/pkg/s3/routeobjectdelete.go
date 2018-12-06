package s3

import (
	"github.com/emicklei/go-restful"
)

func (s *APIService) RouteObjectDelete(request *restful.Request, response *restful.Response) {

	if IsQuery(request, "uploadId") {
		s.AbortMultipartUpload(request,response)
	}else{
		s.ObjectDelete(request,response)
	}
}
