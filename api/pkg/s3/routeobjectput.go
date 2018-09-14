package s3

import (
	"github.com/emicklei/go-restful"
	//	"github.com/micro/go-micro/errors"
)

func (s *APIService) RouteObjectPut(request *restful.Request, response *restful.Response) {
	if IsQuery(request, "acl") {
		//TODO
	} else if IsQuery(request, "tagging") {
		//TODO
	} else if IsQuery(request, "partNumber") {
		//TODO
	} else if HasHeader(request, "x-amz-copy-source") {

	} else {
		s.ObjectPut(request, response)
	}

}
