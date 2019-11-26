package s3

import (
	"github.com/emicklei/go-restful"
	"github.com/opensds/multi-cloud/api/pkg/common"
	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"

	. "github.com/opensds/multi-cloud/s3/error"
)

func (s *APIService) AbortMultipartUpload(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	objectKey := request.PathParameter("objectKey")
	uploadId := request.QueryParameter("uploadId")

	multipartUpload := pb.MultipartUpload{}
	multipartUpload.Key = objectKey
	multipartUpload.Bucket = bucketName
	multipartUpload.UploadId = uploadId

	ctx := common.InitCtxWithAuthInfo(request)
	result, err := s.s3Client.AbortMultipartUpload(ctx, &pb.AbortMultipartRequest{BucketName:bucketName,ObjectKey:objectKey,UploadId:uploadId})
	if err != nil || result.ErrorCode != int32(ErrNoErr) {
		log.Errorln("unable to init multipart. err:", err)
		WriteErrorResponse(response, request, GetFinalError(err, result.ErrorCode))
		return
	}

	WriteSuccessNoContent(response)
	log.Infof("Abort multipart upload[bucketName=%s, objectKey=%s, uploadId=%s] successfully.\n",
		bucketName, objectKey, uploadId)
}
