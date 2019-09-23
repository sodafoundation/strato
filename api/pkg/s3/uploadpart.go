package s3

import (
	"encoding/xml"
	"net/http"
	"strconv"
	"time"

	"github.com/emicklei/go-restful"
	log "github.com/sirupsen/logrus"
	"github.com/opensds/multi-cloud/api/pkg/common"
	"github.com/opensds/multi-cloud/api/pkg/s3/datastore"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	"github.com/opensds/multi-cloud/s3/proto"
)

func (s *APIService) UploadPart(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	objectKey := request.PathParameter("objectKey")
	contentLenght := request.HeaderParameter("content-length")
	size, _ := strconv.ParseInt(contentLenght, 10, 64)
	uploadId := request.QueryParameter("uploadId")
	partNumber := request.QueryParameter("partNumber")
	partNumberInt, _ := strconv.ParseInt(partNumber, 10, 64)
	log.Infof("upload part, partNum=#%s, object=%s, bucket=%s \n", partNumber, objectKey, bucketName)

	md := map[string]string{common.REST_KEY_OPERATION: common.REST_VAL_MULTIPARTUPLOAD}
	ctx := common.InitCtxWithVal(request, md)
	objectInput := s3.GetObjectInput{Bucket: bucketName, Key: objectKey}
	objectMD, _ := s.s3Client.GetObject(ctx, &objectInput)
	lastModified := time.Now().Unix()
	object := s3.Object{}
	object.ObjectKey = objectKey
	object.BucketName = bucketName
	object.LastModified = lastModified
	object.Size = size
	var client datastore.DataStoreAdapter
	if objectMD == nil {
		log.Errorf("no such object err\n")
		response.WriteError(http.StatusInternalServerError, NoSuchObject.Error())

	}
	log.Errorf("objectMD.Backend is %v\n", objectMD.Backend)
	client = getBackendByName(ctx, s, objectMD.Backend)
	if client == nil {
		response.WriteError(http.StatusInternalServerError, NoSuchBackend.Error())
		return
	}
	multipartUpload := s3.MultipartUpload{}
	multipartUpload.Bucket = bucketName
	multipartUpload.Key = objectKey
	multipartUpload.UploadId = uploadId
	log.Infof("call .UploadPart api")
	//call API
	res, s3err := client.UploadPart(request.Request.Body, &multipartUpload, partNumberInt, request.Request.ContentLength, ctx)
	if s3err != NoError {
		response.WriteError(http.StatusInternalServerError, s3err.Error())
		return
	}

	partion := s3.Partion{}

	partion.PartNumber = partNumber
	log.Infof("uploadPart size is %v", size)
	partion.Size = size
	timestamp := time.Now().Unix()
	partion.LastModified = timestamp
	partion.Key = objectKey
	log.Infof("objectMD.Size1 = %v", objectMD.Size)
	objectMD.Size = objectMD.Size + size
	log.Infof("objectMD.Size2 = %v", objectMD.Size)
	objectMD.LastModified = lastModified
	objectMD.Partions = append(objectMD.Partions, &partion)
	//insert metadata
	_, err := s.s3Client.CreateObject(ctx, objectMD)
	result, _ := s.s3Client.GetObject(ctx, &objectInput)
	log.Infof("result.size = %v", result.Size)
	if err != nil {
		log.Errorf("err is %v\n", err)
		response.WriteError(http.StatusInternalServerError, err)
	}

	//return xml format
	xmlstring, err := xml.MarshalIndent(res, "", "  ")
	if err != nil {
		log.Errorf("Parse ListBuckets error: %v", err)
		response.WriteError(http.StatusInternalServerError, err)
		return
	}
	xmlstring = []byte(xml.Header + string(xmlstring))
	log.Infof("resp:\n%s", xmlstring)
	response.Write(xmlstring)

	log.Info("Uploadpart successfully.")
}
