package s3

import (
	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"
	"net/http"
	//	"github.com/micro/go-micro/errors"
	"github.com/opensds/go-panda/s3/proto"
	"golang.org/x/net/context"
)

func (s *APIService) BucketDelete(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	ctx := context.Background()
	log.Logf("Received request for bucket details: %s", bucketName)
	res, err := s.s3Client.DeleteBucket(ctx, &s3.Bucket{Name: bucketName})
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	log.Log("Delete bucket successfully.")
	response.WriteEntity(res)
}
