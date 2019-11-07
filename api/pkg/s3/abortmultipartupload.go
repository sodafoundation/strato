package s3

import (
	"github.com/emicklei/go-restful"
	log "github.com/sirupsen/logrus"
)

func (s *APIService) AbortMultipartUpload(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	objectKey := request.PathParameter("objectKey")
	uploadId := request.QueryParameter("uploadId")

	log.Infof("Abort multipart upload[bucketName=%s, objectKey=%s, uploadId=%s] successfully.\n",
		bucketName, objectKey, uploadId)
}
