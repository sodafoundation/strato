package s3

import (
	"encoding/xml"
	"errors"
	"io/ioutil"
	"sort"
	"strings"

	"github.com/emicklei/go-restful"
	"github.com/opensds/multi-cloud/api/pkg/common"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"

	. "github.com/opensds/multi-cloud/s3/error"
)

func (s *APIService) CompleteMultipartUpload(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	objectKey := request.PathParameter("objectKey")
	uploadId := request.QueryParameter("uploadId")

	multipartUpload := pb.MultipartUpload{}
	multipartUpload.Bucket = bucketName
	multipartUpload.Key = objectKey
	multipartUpload.UploadId = uploadId

	completeMultipartBytes, err := ioutil.ReadAll(request.Request.Body)
	if err != nil {
		log.Errorln("unable to complete multipart upload when read request body. err:", err)
		WriteErrorResponse(response, request, ErrInternalError)
		return
	}
	complMultipartUpload := &model.CompleteMultipartUpload{}
	if err = xml.Unmarshal(completeMultipartBytes, complMultipartUpload); err != nil {
		log.Errorf("unable to parse complete multipart upload XML. data: %s, err: %v", string(completeMultipartBytes), err)
		WriteErrorResponse(response, request, ErrMalformedXML)
		return
	}

	if len(complMultipartUpload.Parts) == 0 {
		log.Errorf("unable to complete multipart upload. err: %v", errors.New("len(complMultipartUpload.Parts) == 0"))
		WriteErrorResponse(response, request, ErrMalformedXML)
		return
	}
	if !sort.IsSorted(model.CompletedParts(complMultipartUpload.Parts)) {
		log.Errorf("unable to complete multipart upload. data: %+v, err: %v", complMultipartUpload.Parts, errors.New("part not sorted."))
		WriteErrorResponse(response, request, ErrInvalidPartOrder)
		return
	}

	// Complete parts.
	var completeParts []*pb.CompletePart
	for _, part := range complMultipartUpload.Parts {
		part.ETag = strings.TrimPrefix(part.ETag, "\"")
		part.ETag = strings.TrimSuffix(part.ETag, "\"")
		completeParts = append(completeParts, &pb.CompletePart{PartNumber: part.PartNumber, ETag: part.ETag})
	}

	ctx := common.InitCtxWithAuthInfo(request)
	result, err := s.s3Client.CompleteMultipartUpload(ctx, &pb.CompleteMultipartRequest{
		BucketName:    bucketName,
		ObjectKey:     objectKey,
		UploadId:      uploadId,
		CompleteParts: completeParts,
	})
	if HandleS3Error(response, request, err, result.GetErrorCode()) != nil {
		log.Errorf("unable to complete multipart. err:%v, errCode:%v", err, result.ErrorCode)
		return
	}

	// Get object location.
	location := GetLocation(request.Request)
	// Generate complete multipart response.
	data := GenerateCompleteMultipartUploadResponse(bucketName, objectKey, location, result.ETag)
	encodedSuccessResponse, err := xmlFormat(data)
	if err != nil {
		log.Errorln("unable to parse CompleteMultipartUpload response, err:", err)
		WriteErrorResponseNoHeader(response, request, ErrInternalError, request.Request.URL.Path)
		return
	}

	setXmlHeader(response, encodedSuccessResponse)
	// write success response.
	WriteSuccessResponse(response, encodedSuccessResponse)
	log.Infof("Complete multipart upload[bucketName=%s, objectKey=%s, uploadId=%s] successfully.\n",
		bucketName, objectKey, uploadId)
}
