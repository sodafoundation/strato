package s3

import (
	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"
	. "github.com/opensds/go-panda/s3/pkg/exception"
	"github.com/opensds/go-panda/s3/proto"
	"golang.org/x/net/context"
	"net/http"
	//	"github.com/micro/go-micro/errors"
)

func (s *APIService) ObjectDelete(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	objectKey := request.PathParameter("objectKey")

	ctx := context.Background()
	log.Logf("Received request for delete object: %s", objectKey)
	deleteInput := s3.DeleteObjectInput{Key: objectKey, Bucket: bucketName}
	client := _getBackendClient(s, bucketName)

	s3err := client.DELETE(&deleteInput, ctx)
	if s3err.Code != ERR_OK {
		response.WriteError(http.StatusInternalServerError, s3err.Error())
		return
	}

	res, err := s.s3Client.DeleteObject(ctx, &deleteInput)
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	log.Logf("Delete object %s successfully.", objectKey)
	response.WriteEntity(res)
}
