package s3

import (
	"context"
	"encoding/xml"
	"github.com/opensds/multi-cloud/api/pkg/s3/datastore"
	"net/http"
	"strconv"
	"time"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-log"
	"github.com/opensds/multi-cloud/s3/proto"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
)

func (s *APIService) UploadPart(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	objectKey := request.PathParameter("objectKey")
	contentLenght := request.HeaderParameter("content-length")
	size, _ := strconv.ParseInt(contentLenght, 10, 64)
	//指定backend功能
	backendName:=request.HeaderParameter("x-amz-storage-class")
	uploadId := request.QueryParameter("uploadId")
	partNumber := request.QueryParameter("partNumber")
	partNumberInt, _ := strconv.ParseInt(partNumber, 10, 64)
	ctx := context.WithValue(request.Request.Context(), "operation", "multipartupload")

	lastModified := time.Now().String()[:19]
	object := s3.Object{}
	object.ObjectKey = objectKey
	object.BucketName = bucketName
	object.LastModified  = lastModified
	object.Size = size
	var client datastore.DataStoreAdapter
	if backendName!=""{
		object.Backend = backendName
		client = getBackendByName(s,backendName)
	}else {
		bucket, _ := s.s3Client.GetBucket(ctx, &s3.Bucket{Name: bucketName})
		object.Backend = bucket.Backend
		client = getBackendClient(s, bucketName)
	}
	if client == nil {
		response.WriteError(http.StatusInternalServerError, NoSuchBackend.Error())
		return
	}
	multipartUpload := s3.MultipartUpload{}
	multipartUpload.Bucket = bucketName
	multipartUpload.Key = objectKey
	multipartUpload.UploadId = uploadId
	//调用分段上传API
	res, s3err := client.UploadPart(request.Request.Body, &multipartUpload, partNumberInt, request.Request.ContentLength, ctx)
	if s3err != NoError {
		response.WriteError(http.StatusInternalServerError, s3err.Error())
		return
	}
	objectInput := s3.GetObjectInput{Bucket: bucketName, Key: objectKey}
	objectMD, _ := s.s3Client.GetObject(ctx, &objectInput)
	partion := s3.Partion{}

	partion.PartNumber = partNumber
	partion.Size = size
	timestamp := time.Now().Unix()
	partion.LastModified = timestamp
	partion.Key = objectKey

	if objectMD != nil {
		objectMD.Size = objectMD.Size + size
		objectMD.LastModified = lastModified
		objectMD.Partions=append(objectMD.Partions, &partion)
		//插入元数据
		_, err := s.s3Client.CreateObject(ctx, objectMD)
		if err!=nil{
			log.Logf("err is %v\n",err)
			response.WriteError(http.StatusInternalServerError, err)
		}
	}else{
		//插入元数据
		object.Partions=append(object.Partions, &partion)
		_, err := s.s3Client.CreateObject(ctx, &object)
		if err!=nil{
			log.Logf("err is %v\n",err)
			response.WriteError(http.StatusInternalServerError, err)
		}
	}



	//返回xml格式
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
