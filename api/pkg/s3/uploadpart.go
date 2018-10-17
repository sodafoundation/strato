package s3

import (
	"context"

	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/emicklei/go-restful"
	"github.com/go-log/log"
	s3 "github.com/opensds/multi-cloud/s3/proto"

	//	"github.com/micro/go-micro/errors"

	. "github.com/opensds/multi-cloud/s3/pkg/exception"
)

func (s *APIService) UploadPart(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	objectKey := request.PathParameter("objectKey")
	uploadId := request.PathParameter("uploadId")

	partNumber := request.PathParameter("partNumber")
	partNumberInt, _ := strconv.ParseInt(partNumber, 10, 64)
	ctx := context.WithValue(request.Request.Context(), "operation", "multipartupload")
  
	multipartUpload := s3.MultipartUpload{}
	multipartUpload.Bucket = bucketName
	multipartUpload.Key = objectKey
	multipartUpload.UploadId = uploadId

	stream := request.Request.Body
	bytes, _ := ioutil.ReadAll(stream)
	len := len(bytes)

	client := _getBackendClient(s, bucketName)
	if client == nil {
		response.WriteError(http.StatusInternalServerError, NoSuchBackend.Error())
		return
	}

	res, s3err := client.UPLOADPART(request.Request.Body, &multipartUpload, partNumberInt, int64(len), ctx)

	if s3err != NoError {
		response.WriteError(http.StatusInternalServerError, s3err.Error())
		return
	}
	log.Log("Uploadpart successfully.")
	response.WriteEntity(res)

}
