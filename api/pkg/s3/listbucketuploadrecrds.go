package s3

import (
	"net/url"
	"sort"

	"github.com/emicklei/go-restful"
	"github.com/opensds/multi-cloud/api/pkg/common"
	"github.com/opensds/multi-cloud/api/pkg/s3/datatype"
	s3error "github.com/opensds/multi-cloud/s3/error"
	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

func (s *APIService) ListBucketUploadRecords(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")

	log.Infof("Received request for listing multipart uploads records: %s\n", bucketName)

	parameters, err := parseListUploadsQuery(request.Request.URL.Query())
	if err != nil {
		log.Errorln("failed to parse list upload query parameter. err:", err)
		WriteErrorResponse(response, request, err)
		return
	}

	ctx := common.InitCtxWithAuthInfo(request)
	listMultipartsResponse, err := s.s3Client.ListBucketUploadRecords(ctx, &pb.ListBucketUploadRequest{
		BucketName:     bucketName,
		Delimiter:      parameters.Delimiter,
		EncodingType:   parameters.EncodingType,
		MaxUploads:     int32(parameters.MaxUploads),
		KeyMarker:      parameters.KeyMarker,
		Prefix:         parameters.Prefix,
		UploadIdMarker: parameters.UploadIdMarker,
	})
	if err != nil || listMultipartsResponse.ErrorCode != int32(s3error.ErrNoErr) {
		log.Errorf("Unable to list multipart uploads. err:", err)
		WriteErrorResponse(response, request, err)
		return
	}

	resData := datatype.ListMultipartUploadsResponse{}
	resData.IsTruncated = listMultipartsResponse.Result.IsTruncated
	resData.NextKeyMarker = listMultipartsResponse.Result.NextKeyMarker
	resData.NextUploadIdMarker = listMultipartsResponse.Result.NextUploadIdMarker
	sort.Strings(listMultipartsResponse.Result.CommonPrefix)
	for _, prefix := range listMultipartsResponse.Result.CommonPrefix {
		resData.CommonPrefixes = append(resData.CommonPrefixes, datatype.CommonPrefix{
			Prefix: prefix,
		})
	}
	for _, upload := range listMultipartsResponse.Result.Uploads {
		resData.Uploads = append(resData.Uploads, datatype.Upload{
			Key:      upload.Key,
			UploadId: upload.UploadId,
			Initiator: datatype.Initiator{
				ID:          upload.Initiator.Id,
				DisplayName: upload.Initiator.DisplayName,
			},
			Owner: datatype.Owner{
				ID:          upload.Owner.Id,
				DisplayName: upload.Owner.DisplayName,
			},
			StorageClass: upload.StorageClass,
			Initiated:    upload.Initiated,
		})
	}

	resData.Bucket = bucketName
	resData.KeyMarker = parameters.KeyMarker
	resData.UploadIdMarker = parameters.UploadIdMarker
	resData.MaxUploads = parameters.MaxUploads
	resData.Prefix = parameters.Prefix
	resData.Delimiter = parameters.Delimiter
	resData.EncodingType = parameters.EncodingType
	if resData.EncodingType != "" { // only support "url" encoding for now
		resData.Delimiter = url.QueryEscape(resData.Delimiter)
		resData.KeyMarker = url.QueryEscape(resData.KeyMarker)
		resData.Prefix = url.QueryEscape(resData.Prefix)
		resData.NextKeyMarker = url.QueryEscape(resData.NextKeyMarker)
		for _, upload := range resData.Uploads {
			upload.Key = url.QueryEscape(upload.Key)
		}
	}

	encodedSuccessResponse := EncodeResponse(resData)
	// write success response.
	WriteSuccessResponse(response, encodedSuccessResponse)
	log.Infoln("List bucket multipart uploads successfully.")
}
