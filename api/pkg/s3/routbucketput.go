package s3

import (
	"github.com/emicklei/go-restful"
)

func (s *APIService) RouteBucketPut(request *restful.Request, response *restful.Response) {
	if IsQuery(request, "acl") {
		//TODO
	} else if IsQuery(request, "versioning") {
		//TODO
	} else if IsQuery(request, "website") {
		//TODO
	} else if IsQuery(request, "cors") {
		//TODO

	} else if IsQuery(request, "replication") {
		//TODO

	} else if IsQuery(request, "lifecycle") {
		//TODO

	} else {
		s.BucketPut(request, response)
	}
}
