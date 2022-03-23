// Copyright 2019 The soda Authors.
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
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strconv"

	"github.com/soda/multi-cloud/api/pkg/common"
	"github.com/soda/multi-cloud/api/pkg/filters/signature"
	s3error "github.com/soda/multi-cloud/s3/error"
	s3 "github.com/soda/multi-cloud/s3/proto"

	"github.com/emicklei/go-restful"
	log "github.com/sirupsen/logrus"
)

func (s *APIService) UploadPart(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	objectKey := request.PathParameter("objectKey")

	var incomingMd5 string
	// get Content-Md5 sent by client and verify if valid
	md5Bytes, err := checkValidMD5(request.HeaderParameter(common.REQUEST_HEADER_CONTENT_MD5))
	if err != nil {
		incomingMd5 = ""
	} else {
		incomingMd5 = hex.EncodeToString(md5Bytes)
	}

	size := request.Request.ContentLength
	/// maximum Upload size for multipart objects in a single operation
	if isMaxObjectSize(size) {
		log.Errorf("the size of object to upload is too large.")
		WriteErrorResponse(response, request, s3error.ErrEntityTooLarge)
		return
	}
	log.Infoln("uploadpart size:", size)

	uploadID := request.QueryParameter("uploadId")
	partIDString := request.QueryParameter("partNumber")
	partID, err := strconv.Atoi(partIDString)
	if err != nil {
		log.Errorf("failed to convert part id string to integer")
		WriteErrorResponse(response, request, s3error.ErrInvalidPart)
		return
	}
	// check partID with maximum part ID for multipart objects
	if isMaxPartID(partID) {
		log.Errorf("the part id is invalid.")
		WriteErrorResponse(response, request, s3error.ErrInvalidMaxParts)
		return
	}

	ctx := common.InitCtxWithAuthInfo(request)
	stream, err := s.s3Client.UploadPart(ctx)
	defer stream.Close()

	uploadRequest := s3.UploadPartRequest{
		BucketName: bucketName,
		ObjectKey:  objectKey,
		UploadId:   uploadID,
		PartId:     int32(partID),
		Size:       size,
		Md5Hex:     incomingMd5,
	}
	err = stream.SendMsg(&uploadRequest)
	if err != nil {
		log.Errorln("failed send upload request msg. err:", err)
		WriteErrorResponse(response, request, err)
		return
	}

	var limitedDataReader io.Reader
	if size > 0 { // request.ContentLength is -1 if length is unknown
		limitedDataReader = io.LimitReader(request.Request.Body, size)
	} else {
		limitedDataReader = request.Request.Body
	}
	dataReader := limitedDataReader
	// Build sha256sum if needed.
	inputSha256Sum := request.Request.Header.Get("X-Amz-Content-Sha256")
	sha256Writer := sha256.New()
	needCheckSha256 := false
	if inputSha256Sum != "" && inputSha256Sum != signature.UnsignedPayload {
		needCheckSha256 = true
		dataReader = io.TeeReader(limitedDataReader, sha256Writer)
	}

	buf := make([]byte, ChunkSize)
	eof := false
	for !eof {
		n, err := dataReader.Read(buf)
		if err != nil && err != io.EOF {
			log.Errorf("read error:%v\n", err)
			break
		}
		if err == io.EOF {
			log.Debugln("finished read")
			eof = true
		}

		err = stream.Send(&s3.PutDataStream{Data: buf[:n]})
		if err != nil {
			log.Infof("stream send error: %v\n", err)
			break
		}
	}

	result := s3.UploadPartResponse{}
	err = stream.RecvMsg(&result)
	if HandleS3Error(response, request, err, result.GetErrorCode()) != nil {
		log.Errorln("unable to recv message. err:, errcode:", err, result.ErrorCode)
		return
	}

	// Check if sha256sum match.
	if needCheckSha256 {
		sha256Sum := hex.EncodeToString(sha256Writer.Sum(nil))
		if inputSha256Sum != sha256Sum {
			log.Errorln("sha256Sum:", sha256Sum, ", received:",
				request.Request.Header.Get("X-Amz-Content-Sha256"))
			WriteErrorResponse(response, request, s3error.ErrContentSHA256Mismatch)
		}
	}

	if result.ETag != "" {
		response.Header()["ETag"] = []string{"\"" + result.ETag + "\""}
	}

	WriteSuccessResponse(response, nil)
	log.Info("Uploadpart successfully.")
}
