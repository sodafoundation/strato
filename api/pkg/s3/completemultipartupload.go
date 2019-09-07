package s3

import (
	"context"
	"encoding/xml"
	"net/http"
	"time"

	"github.com/opensds/multi-cloud/api/pkg/s3/datastore"

	"github.com/emicklei/go-restful"
	log "github.com/sirupsen/logrus"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	s3 "github.com/opensds/multi-cloud/s3/proto"
)

func (s *APIService) CompleteMultipartUpload(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	objectKey := request.PathParameter("objectKey")
	UploadId := request.QueryParameter("uploadId")
	ctx := context.WithValue(request.Request.Context(), "operation", "multipartupload")
	objectInput := s3.GetObjectInput{Bucket: bucketName, Key: objectKey}
	objectMD, _ := s.s3Client.GetObject(ctx, &objectInput)
	//to insert object
	object := s3.Object{}
	object.BucketName = bucketName
	object.ObjectKey = objectKey
	multipartUpload := s3.MultipartUpload{}
	multipartUpload.Bucket = bucketName
	multipartUpload.Key = objectKey
	multipartUpload.UploadId = UploadId

	body := ReadBody(request)

	log.Infof("complete multipart upload body: %s", string(body))
	completeUpload := &model.CompleteMultipartUpload{}
	xml.Unmarshal(body, completeUpload)
	var client datastore.DataStoreAdapter
	if objectMD == nil {
		log.Errorf("No such object err\n")
		response.WriteError(http.StatusInternalServerError, NoSuchObject.Error())

	}
	client = getBackendByName(s, objectMD.Backend)
	if client == nil {
		response.WriteError(http.StatusInternalServerError, NoSuchBackend.Error())
		return
	}

	resp, s3err := client.CompleteMultipartUpload(&multipartUpload, completeUpload, ctx)
	log.Infof("resp is %v\n", resp)
	if s3err != NoError {
		response.WriteError(http.StatusInternalServerError, s3err.Error())
		return
	}

	// delete multipart upload record, if delete failed, it will be cleaned by lifecycle management
	record := s3.MultipartUploadRecord{ObjectKey: objectKey, Bucket: bucketName, UploadId: UploadId}
	s.s3Client.DeleteUploadRecord(context.Background(), &record)

	objectMD.Partions = nil
	objectMD.LastModified = time.Now().Unix()
	objectMD.InitFlag = "1"
	//insert metadata
	_, err := s.s3Client.CreateObject(ctx, objectMD)
	if err != nil {
		log.Errorf("err is %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
	}

	xmlstring, err := xml.MarshalIndent(resp, "", "  ")
	if err != nil {
		log.Errorf("Parse ListBuckets error: %v", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}

	xmlstring = []byte(xml.Header + string(xmlstring))
	log.Infof("resp:\n%s", xmlstring)
	response.Write(xmlstring)
}
