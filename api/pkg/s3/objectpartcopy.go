// Copyright 2019 The OpenSDS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package s3

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/emicklei/go-restful"
	"github.com/journeymidnight/yig/helper"
	"github.com/opensds/multi-cloud/api/pkg/common"
	"github.com/opensds/multi-cloud/api/pkg/s3/datatype"
	. "github.com/opensds/multi-cloud/s3/error"

	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

// ObjectPartCopy copy object from http header x-amz-copy-source as a part for multipart
func (s *APIService) ObjectPartCopy(request *restful.Request, response *restful.Response) {
	targetBucketName := request.PathParameter(common.REQUEST_PATH_BUCKET_NAME)
	targetObjectName := request.PathParameter(common.REQUEST_PATH_OBJECT_KEY)

	log.Infof("received request: Copy object part, target bucket[%v], target object[%s]", targetBucketName, targetObjectName)

	if !isValidObjectName(targetObjectName) {
		log.Errorln("target object name is invalid. ")
		WriteErrorResponse(response, request, ErrInvalidObjectName)
		return
	}

	uploadID := request.QueryParameter("uploadId")
	partIDString := request.QueryParameter("partNumber")
	partID, err := strconv.Atoi(partIDString)
	if err != nil {
		log.Errorf("failed to convert part id string[%s] to integer, err:%v", partIDString, err)
		WriteErrorResponse(response, request, ErrInvalidPart)
		return
	}
	// check partID with maximum part ID for multipart objects
	if isMaxPartID(partID) {
		log.Errorln("part ID is greater than the maximum allowed ID.")
		WriteErrorResponse(response, request, ErrInvalidMaxParts)
		return
	}

	// copy source is of form: /bucket-name/object-name?versionId=xxxxxx
	copySource := request.HeaderParameter(common.REQUEST_HEADER_COPY_SOURCE)

	// Skip the first element if it is '/', split the rest.
	if strings.HasPrefix(copySource, "/") {
		copySource = copySource[1:]
	}
	splits := strings.SplitN(copySource, "/", 2)

	// Save sourceBucket and sourceObject extracted from url Path.
	var sourceBucketName, sourceObjectName string
	if len(splits) == 2 {
		sourceBucketName = splits[0]
		sourceObjectName = splits[1]
	} else {
		log.Infoln("copy source should be splited at least two parts.")
		WriteErrorResponse(response, request, ErrInvalidCopySource)
		return
	}
	// If source object is empty, reply back error.
	if sourceBucketName == "" || sourceObjectName == "" {
		log.Errorln("there is no string for sourceBucketName or sourceObjectName.")
		WriteErrorResponse(response, request, ErrInvalidCopySource)
		return
	}

	splits = strings.SplitN(sourceObjectName, "?", 2)
	if len(splits) > 1 {
		// we dont support source object name that has "?"
		log.Errorln("we dont support source object name that has ?")
		WriteErrorResponse(response, request, ErrNotImplemented)
		return
	}

	// X-Amz-Copy-Source should be URL-encoded
	sourceBucketName, err = url.QueryUnescape(sourceBucketName)
	if err != nil {
		log.Errorln("failed to QueryUnescape. err:", err)
		WriteErrorResponse(response, request, ErrInvalidCopySource)
		return
	}
	sourceObjectName, err = url.QueryUnescape(sourceObjectName)
	if err != nil {
		log.Errorln("failed to QueryUnescape. err:", err)
		WriteErrorResponse(response, request, ErrInvalidCopySource)
		return
	}

	ctx := common.InitCtxWithAuthInfo(request)
	getObjMetaRes, err := s.s3Client.GetObjectMeta(ctx, &pb.GetObjectMetaRequest{
		ObjectKey:  sourceObjectName,
		BucketName: sourceBucketName,
	})
	if HandleS3Error(response, request, err, getObjMetaRes.GetErrorCode()) != nil {
		log.Errorf("unable to fetch object meta. err:%v, errCode:%v", err, getObjMetaRes.ErrorCode)
		return
	}

	// Verify before x-amz-copy-source preconditions before continuing with CopyObject.
	if err = checkObjectPreconditions(response.ResponseWriter, request.Request, getObjMetaRes.Object); err != nil {
		log.Infof("failed to check object preconditions. err:", err)
		WriteErrorResponse(response, request, err)
		return
	}

	var readOffset, readLength int64
	copySourceRangeString := request.HeaderParameter(common.REQUEST_HEADER_COPY_SOURCE_RANGE)
	if copySourceRangeString == "" {
		readOffset = 0
		readLength = getObjMetaRes.Object.Size
	} else {
		copySourceRange, err := datatype.ParseRequestRange(copySourceRangeString, getObjMetaRes.Object.Size)
		if err != nil {
			helper.ErrorIf(err, "Invalid request range, err:", err)
			WriteErrorResponse(response, request, ErrInvalidRange)
			return
		}
		readOffset = copySourceRange.OffsetBegin
		readLength = copySourceRange.GetLength()
	}
	if isMaxObjectSize(readLength) {
		log.Errorf("object size is too large. size:%v", readLength)
		WriteErrorResponseWithResource(response, request, ErrEntityTooLarge, copySource)
		return
	}

	result, err := s.s3Client.CopyObjPart(ctx, &pb.CopyObjPartRequest{
		TargetBucket: targetBucketName,
		TargetObject: targetObjectName,
		SourceObject: sourceObjectName,
		SourceBucket: sourceBucketName,
		UploadID:     uploadID,
		PartID:       int64(partID),
		ReadOffset:   readOffset,
		ReadLength:   readLength,
	})
	if HandleS3Error(response, request, err, result.ErrorCode) != nil {
		log.Errorf("failed to copy object part s3. err:%v, errCode:%v", err, result.ErrorCode)
		return
	}

	data := GenerateCopyObjectPartResponse(result.Etag, result.LastModified)
	encodedSuccessResponse := EncodeResponse(data)
	// write headers
	if result.Etag != "" {
		response.ResponseWriter.Header()["ETag"] = []string{"\"" + result.Etag + "\""}
	}
	log.Infoln("copy object part successfully.")
	// write success response.
	WriteSuccessResponse(response, encodedSuccessResponse)
}
