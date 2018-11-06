package s3

import (
	"context"
	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"
	"net/http"
	//	"github.com/micro/go-micro/errors"

	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	"github.com/opensds/multi-cloud/s3/proto"
)

func (s *APIService) AbortMultipartUpload(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	objectKey := request.PathParameter("objectKey")
	uploadId := request.QueryParameter("uploadId")
	ctx := context.WithValue(request.Request.Context(), "operation", "multipartupload")
	multipartUpload := s3.MultipartUpload{}
	multipartUpload.Key = objectKey
	multipartUpload.Bucket = bucketName
	multipartUpload.UploadId = uploadId

	client := getBackendClient(s, bucketName)
	if client == nil {
		response.WriteError(http.StatusInternalServerError, NoSuchBackend.Error())
		return
	}
	s3err := client.AbortMultipartUpload(&multipartUpload, ctx)
	if s3err != NoError {
		response.WriteError(http.StatusInternalServerError, s3err.Error())
		return
	}
	objectInput := s3.GetObjectInput{Bucket: bucketName, Key: objectKey}
	objectMD, _ := s.s3Client.GetObject(ctx, &objectInput)
	if objectMD != nil {
		deleteInput := s3.DeleteObjectInput{Key: objectKey, Bucket: bucketName}
		res, err := s.s3Client.DeleteObject(ctx, &deleteInput)
		if err != nil {
			response.WriteError(http.StatusInternalServerError, err)
			return
		}
		log.Logf("Delete object %s successfully.", objectKey)
		response.WriteEntity(res)
	} else {
		response.WriteError(http.StatusInternalServerError, InternalError.Error())

	}
}
