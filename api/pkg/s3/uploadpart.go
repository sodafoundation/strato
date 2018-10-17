package s3

import (
	"context"

	"net/http"
	"strconv"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"
	"github.com/opensds/multi-cloud/s3/proto"

	//	"github.com/micro/go-micro/errors"

	"encoding/xml"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
)

func (s *APIService) UploadPart(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	objectKey := request.PathParameter("objectKey")

	uploadId := request.QueryParameter("uploadId")
	partNumber := request.QueryParameter("partNumber")
	partNumberInt, _ := strconv.ParseInt(partNumber, 10, 64)
	ctx := context.WithValue(request.Request.Context(), "operation", "multipartupload")

	multipartUpload := s3.MultipartUpload{}
	multipartUpload.Bucket = bucketName
	multipartUpload.Key = objectKey
	multipartUpload.UploadId = uploadId

	client := _getBackendClient(s, bucketName)
	if client == nil {
		response.WriteError(http.StatusInternalServerError, NoSuchBackend.Error())
		return
	}

	res, s3err := client.UploadPart(request.Request.Body, &multipartUpload, partNumberInt, request.Request.ContentLength, ctx)
	if s3err != NoError {
		response.WriteError(http.StatusInternalServerError, s3err.Error())
		return
	}

	xmlstring, err := xml.MarshalIndent(res, "", "  ")
	if err != nil {
		log.Logf("Parse ListBuckets error: %v", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	xmlstring = []byte(xml.Header + string(xmlstring))
	log.Logf("resp:\n%s", xmlstring)
	response.Write(xmlstring)

	log.Log("Uploadpart successfully.")
}
