package s3

import (
	"context"
	"net/http"
	"strconv"

	"github.com/emicklei/go-restful"

	//	"github.com/micro/go-micro/errors"

	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	s3 "github.com/opensds/multi-cloud/s3/proto"
)

func (s *APIService) AbortMultipartUpload(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	objectKey := request.PathParameter("objectKey")
	uploadId := request.PathParameter("uploadId")
	contentLenght := request.HeaderParameter("content-length")
	ctx := context.WithValue(request.Request.Context(), "operation", "MultipartUpload")
	object := s3.Object{}
	multipartUpload := s3.MultipartUpload{}
	multipartUpload.Key = objectKey
	multipartUpload.Bucket = bucketName
	multipartUpload.UploadId = uploadId
	size, _ := strconv.ParseInt(contentLenght, 10, 64)
	object.Size = size

	client := _getBackendClient(s, bucketName)
	if client == nil {
		response.WriteError(http.StatusInternalServerError, NoSuchBackend.Error())
		return
	}
	s3err := client.ABORTMULTIPARTUPLOAD(&multipartUpload, ctx)
	if s3err != NoError {
		response.WriteError(http.StatusInternalServerError, s3err.Error())
		return
	}
}
