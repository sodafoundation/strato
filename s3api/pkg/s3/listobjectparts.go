package s3

import (
	"github.com/emicklei/go-restful"
	pb "github.com/opensds/multi-cloud/s3/proto"
	"github.com/opensds/multi-cloud/s3api/pkg/common"
	"github.com/opensds/multi-cloud/s3api/pkg/s3/datatype"
	log "github.com/sirupsen/logrus"
)

func (s *APIService) ListObjectParts(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	objectKey := request.PathParameter("objectKey")
	log.Infof("received request: list object multipart, bucketet[name=%s] object[name=%s]\n", bucketName, objectKey)

	listPartReq, err := parseListObjectPartsQuery(request.Request.URL.Query())
	if err != nil {
		log.Errorln("failed to parse object part query parameter. err:", err)
		WriteErrorResponse(response, request, err)
		return
	}

	ctx := common.InitCtxWithAuthInfo(request)
	listObjectPartsRes, err := s.s3Client.ListObjectParts(ctx, &pb.ListObjectPartsRequest{
		BucketName:       bucketName,
		ObjectKey:        objectKey,
		UploadId:         listPartReq.UploadId,
		EncodingType:     listPartReq.EncodingType,
		MaxParts:         int64(listPartReq.MaxParts),
		PartNumberMarker: int64(listPartReq.PartNumberMarker),
	})
	if err != nil {
		log.Errorln("unable to list uploaded parts. err:", err)
		WriteErrorResponse(response, request, err)
		return
	}

	data := datatype.ListPartsResponse{
		Bucket:       bucketName,
		Key:          objectKey,
		UploadId:     listPartReq.UploadId,
		EncodingType: listPartReq.EncodingType,
		Initiator: datatype.Initiator(datatype.Owner{
			ID:          listObjectPartsRes.Initiator.Id,
			DisplayName: listObjectPartsRes.Initiator.DisplayName,
		}),
		Owner: datatype.Owner{
			ID:          listObjectPartsRes.Owner.Id,
			DisplayName: listObjectPartsRes.Owner.DisplayName,
		},
		PartNumberMarker:     int(listObjectPartsRes.PartNumberMarker),
		NextPartNumberMarker: int(listObjectPartsRes.NextPartNumberMarker),
		MaxParts:             int(listObjectPartsRes.MaxParts),
		IsTruncated:          listObjectPartsRes.IsTruncated,
	}
	data.Parts = make([]datatype.Part, 0)
	for _, part := range listObjectPartsRes.Parts {
		data.Parts = append(data.Parts, datatype.Part{
			PartNumber:   int(part.PartNumber),
			ETag:         part.ETag,
			LastModified: part.LastModified,
			Size:         part.Size,
		})
	}

	encodedSuccessResponse := EncodeResponse(data)
	// Write success response.
	log.Infof("list object parts successfully.")
	WriteSuccessResponse(response, encodedSuccessResponse)
}
