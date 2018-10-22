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
	"strconv"
)

func (s *APIService) CompleteMultipartUpload(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	objectKey := request.PathParameter("objectKey")
	UploadId := request.QueryParameter("uploadId")
	contentLenght := request.HeaderParameter("content-length")

	ctx := context.WithValue(request.Request.Context(), "operation", "multipartupload")
	object := s3.Object{}
	object.ObjectKey = objectKey
	object.BucketName = bucketName
	size, _ := strconv.ParseInt(contentLenght, 10, 64)
	object.Size = size
	multipartUpload := s3.MultipartUpload{}
	multipartUpload.Bucket = bucketName
	multipartUpload.Key = objectKey
	multipartUpload.UploadId = UploadId

	body := ReadBody(request)

	log.Logf("complete multipart upload body: %s", string(body))
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
	objectInput := s3.GetObjectInput{Bucket: bucketName, Key: objectKey}
	objectMD, _ := s.s3Client.GetObject(ctx, &objectInput)
	if objectMD != nil {
		res,err := s.s3Client.UpdateObject(ctx,&object)
		if err != nil {
			response.WriteError(http.StatusInternalServerError, err)
			return
		}
		log.Log("Upload object successfully.")
		response.WriteEntity(res)
	} else {
		res, err := s.s3Client.CreateObject(ctx, &object)
		if err != nil {
			response.WriteError(http.StatusInternalServerError, err)
			return
		}
		log.Log("Upload object successfully.")
		response.WriteEntity(res)
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
