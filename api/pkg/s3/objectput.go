package s3

import (
	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"
	"net/http"
	//	"github.com/micro/go-micro/errors"
	"github.com/opensds/go-panda/api/pkg/s3/datastore"
	. "github.com/opensds/go-panda/s3/pkg/exception"
	"github.com/opensds/go-panda/s3/proto"
	"golang.org/x/net/context"
	"strconv"
)

func _getBackendClient(s *APIService, bucketName string) datastore.DataStoreAdapter {
	ctx := context.Background()
	buekct, err := s.s3Client.GetBucket(ctx, &s3.Bucket{Name: bucketName})
	if err != nil {
		return nil
	}
	client := datastore.Init(buekct.Backend)
	return client
}

//ObjectPut -
func (s *APIService) ObjectPut(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	objectKey := request.PathParameter("objectKey")
	contentLenght := request.HeaderParameter("content-length")

	ctx := context.WithValue(request.Request.Context(), "operation", "upload")

	log.Logf("Received request for bucket details: %s", bucketName)

	object := s3.Object{}
	object.ObjectKey = objectKey
	object.BucketName = bucketName
	size, err := strconv.ParseInt(contentLenght, 10, 64)
	object.Size = size

	client := _getBackendClient(s, bucketName)
	if client == nil {
		response.WriteError(http.StatusInternalServerError, NoSuchBackend.Error())
		return
	}

	s3err := client.PUT(request.Request.Body, &object, ctx)
	if s3err != NoError {
		response.WriteError(http.StatusInternalServerError, s3err.Error())
		return
	}

	res, err := s.s3Client.PutObject(ctx, &object)
	if err != nil {
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	log.Log("Upload object successfully.")
	response.WriteEntity(res)
}
