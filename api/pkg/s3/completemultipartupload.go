package s3

import (
	"context"
	"encoding/xml"
	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	"github.com/opensds/multi-cloud/s3/proto"
	"net/http"
)

func (s *APIService) CompleteMultipartUpload(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	objectKey := request.PathParameter("objectKey")
	UploadId := request.QueryParameter("uploadId")

	ctx := context.WithValue(request.Request.Context(), "operation", "multipartupload")
	multipartUpload := s3.MultipartUpload{}
	multipartUpload.Bucket = bucketName
	multipartUpload.Key = objectKey
	multipartUpload.UploadId = UploadId

	body := ReadBody(request)

	log.Logf("Create bucket body: %s", string(body))
	completeUpload := &model.CompleteMultipartUpload{}
	xml.Unmarshal(body, completeUpload)

	log.Logf("multipartUpload:%v, parts:%v", multipartUpload, completeUpload)

	client := _getBackendClient(s, bucketName)
	if client == nil {
		response.WriteError(http.StatusInternalServerError, NoSuchBackend.Error())
		return
	}
	resp, s3err := client.CompleteMultipartUpload(&multipartUpload, completeUpload, ctx)
	if s3err != NoError {
		response.WriteError(http.StatusInternalServerError, s3err.Error())
		return
	}
	xmlstring, err := xml.MarshalIndent(resp, "", "  ")
	if err != nil {
		log.Logf("Parse ListBuckets error: %v", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	xmlstring = []byte(xml.Header + string(xmlstring))
	log.Logf("resp:\n%s", xmlstring)
	response.Write(xmlstring)
}
