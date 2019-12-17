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
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/emicklei/go-restful"
	"github.com/opensds/multi-cloud/api/pkg/common"
	. "github.com/opensds/multi-cloud/api/pkg/s3/datatype"
	s3error "github.com/opensds/multi-cloud/s3/error"
	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

// GetObjectVersionHandler - GET Object
func (s *APIService) ObjectVersionGet(request *restful.Request, response *restful.Response) {
	bucketName := strings.ToLower(request.PathParameter(common.REQUEST_PATH_BUCKET_NAME))
	objectKey := request.PathParameter(common.REQUEST_PATH_OBJECT_KEY)
	versionId := request.Request.URL.Query().Get(common.REQUEST_QUERY_VERSION_ID)

	log.Infof("received request: GET bucket[name=%s] object[name=%s] acl\n", bucketName, objectKey)

	ctx := common.InitCtxWithAuthInfo(request)
	object, err := s.getObjectMeta(ctx, bucketName, objectKey, versionId)
	if err != nil {
		log.Error("failed to get object[%s] meta", objectKey)
		WriteErrorResponse(response, request, err)
		return
	}

	// Get request range.
	var hrange *HttpRange
	rangeHeader := request.HeaderParameter("Range")
	if rangeHeader != "" {
		if hrange, err = ParseRequestRange(rangeHeader, object.Size); err != nil {
			// Handle only ErrorInvalidRange
			// Ignore other parse error and treat it as regular Get request like Amazon S3.
			if err == ErrorInvalidRange {
				WriteErrorResponse(response, request, s3error.ErrInvalidRange)
				return
			}
			// log the error.
			log.Errorln("invalid request range, err:", err)
		}
	}

	// Validate pre-conditions if any.
	if err = checkPreconditions(request.Request.Header, object); err != nil {
		// set object-related metadata headers
		response.AddHeader("Last-Modified", time.Unix(object.LastModified, 0).UTC().Format(http.TimeFormat))

		if object.Etag != "" {
			response.ResponseWriter.Header()["ETag"] = []string{"\"" + object.Etag + "\""}
		}
		if err == s3error.ContentNotModified { // write only header if is a 304
			WriteErrorResponseHeaders(response, err)
		} else {
			WriteErrorResponse(response, request, err)
		}
		return
	}

	// Get the object.
	startOffset := int64(0)
	length := object.Size
	if hrange != nil {
		startOffset = hrange.OffsetBegin
		length = hrange.GetLength()
	}
	stream, err := s.s3Client.GetObject(ctx, &pb.GetObjectInput{Bucket: bucketName, Key: objectKey+ "_" + versionId, VersionId:versionId, Offset: startOffset, Length: length})
	if err != nil {
		log.Errorln("get object failed, err:%v", err)
		WriteErrorResponse(response, request, err)
		return
	}
	defer stream.Close()

	// Indicates if any data was written to the http.ResponseWriter
	dataWritten := false
	// io.Writer type which keeps track if any data was written.
	writer := func(p []byte) (int, error) {
		if !dataWritten {
			// Set headers on the first write.
			// Set standard object headers.
			SetObjectHeaders(response, object, hrange)

			// Set any additional requested response headers.
			setGetRespHeaders(response.ResponseWriter, request.Request.URL.Query())
			dataWritten = true
		}
		n, err := response.Write(p)
		return n, err
	}

	s3err := int32(s3error.ErrNoErr)
	eof := false
	left := length
	for !eof && left > 0 {
		rsp, err := stream.Recv()
		if err != nil && err != io.EOF {
			log.Errorln("recv err", err)
			break
		}
		// If err is equal to EOF, a non-zero number of bytes may be returned.
		// the err is set EOF, returned data is processed at the subsequent code.
		if err == io.EOF {
			eof = true
		}
		// It indicate that there is a error from grpc server.
		if rsp.ErrorCode != int32(s3error.ErrNoErr) {
			s3err = rsp.ErrorCode
			log.Errorf("received s3 service error, error code:%v", rsp.ErrorCode)
			break
		}
		// If there is no data in rsp.Data, it show that there is no more data to receive
		if len(rsp.Data) == 0 {
			break
		}
		_, err = writer(rsp.Data)
		if err != nil {
			log.Errorln("failed to write data to client. err:", err)
			break
		}
		left -= int64(len(rsp.Data))
	}

	if !dataWritten {
		if s3err == int32(s3error.ErrNoErr) {
			writer(nil)
		} else {
			WriteErrorResponse(response, request, s3error.S3ErrorCode(s3err))
			return
		}
	}

	log.Info("Get object successfully.")
}
