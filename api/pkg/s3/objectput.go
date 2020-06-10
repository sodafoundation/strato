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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/emicklei/go-restful"
	"github.com/opensds/multi-cloud/api/pkg/common"
	"github.com/opensds/multi-cloud/api/pkg/filters/signature"
	"github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/proto"
	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

var ChunkSize int = 2048

//ObjectPut -
func (s *APIService) ObjectPut(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter(common.REQUEST_PATH_BUCKET_NAME)
	objectKey := request.PathParameter(common.REQUEST_PATH_OBJECT_KEY)
	backendName := request.HeaderParameter(common.REQUEST_HEADER_STORAGE_CLASS)
	url := request.Request.URL
	if strings.HasSuffix(url.String(), "/") {
		objectKey = objectKey + "/"
	}
	log.Infof("received request: PUT object, objectkey=%s, bucketName=%s\n:",
		objectKey, bucketName)

	//var authType = signature.GetRequestAuthType(r)
	var err error
	if !isValidObjectName(objectKey) {
		WriteErrorResponse(response, request, s3error.ErrInvalidObjectName)
		return
	}

	// get size
	size, err := getSize(request, response)
	if err != nil {
		return
	}
	if size == -1 {
		WriteErrorResponse(response, request, s3error.ErrMissingContentLength)
		return
	}

	// maximum Upload size for objects in a single operation
	if isMaxObjectSize(size) {
		WriteErrorResponse(response, request, s3error.ErrEntityTooLarge)
		return
	}

	// Save metadata.
	metadata := extractMetadataFromHeader(request.Request.Header)
	// Get Content-Md5 sent by client and verify if valid
	if _, ok := request.Request.Header["Content-Md5"]; !ok {
		metadata["md5Sum"] = ""
	} else {
		if len(request.Request.Header.Get("Content-Md5")) == 0 {
			log.Infoln("Content Md5 is null!")
			WriteErrorResponse(response, request, s3error.ErrInvalidDigest)
			return
		}
		md5Bytes, err := checkValidMD5(request.Request.Header.Get("Content-Md5"))
		if err != nil {
			log.Infoln("Content Md5 is invalid!")
			WriteErrorResponse(response, request, s3error.ErrInvalidDigest)
			return
		} else {
			metadata["md5Sum"] = hex.EncodeToString(md5Bytes)
		}
	}

	acl, err := getAclFromHeader(request)
	if err != nil {
		WriteErrorResponse(response, request, err)
		return
	}

	// check if specific bucket exist
	ctx := common.InitCtxWithAuthInfo(request)
	bucketMeta, err := s.getBucketMeta(ctx, bucketName)
	if err != nil {
		log.Errorln("failed to get bucket meta. err:", err)
		WriteErrorResponse(response, request, err)
		return
	}
	//log.Logf("bucket, acl:%f", bucketMeta.Acl.CannedAcl)
	location := bucketMeta.DefaultLocation
	if backendName != "" {
		// check if backend exist
		if s.isBackendExist(ctx, backendName) == false {
			WriteErrorResponse(response, request, s3error.ErrGetBackendFailed)
			return
		}
		location = backendName
	}

	var limitedDataReader io.Reader
	if size > 0 { // request.ContentLength is -1 if length is unknown
		limitedDataReader = io.LimitReader(request.Request.Body, size)
	} else {
		limitedDataReader = request.Request.Body
	}

	buf := make([]byte, ChunkSize)
	eof := false
	stream, err := s.s3Client.PutObject(ctx)
	defer stream.Close()
	obj := pb.PutObjectRequest{
		BucketName: bucketName,
		ObjectKey:  objectKey,
		Acl:        &pb.Acl{CannedAcl: acl.CannedAcl},
		Attrs:      metadata,
		Location:   location,
		Size:       size,
	}
	// add all header information, if any
	obj.Headers = make(map[string]*pb.HeaderValues, len(request.Request.Header))

	for k, v := range request.Request.Header {
		valueArr := make([]string, 0)
		headerValues := pb.HeaderValues{Values: valueArr}
		// add all v for this k to headerValues
		headerValues.Values = append(headerValues.Values, v...)
		// add k,[v] to input proto struct
		obj.Headers[k] = &headerValues
	}

	err = stream.SendMsg(&obj)
	if err != nil {
		WriteErrorResponse(response, request, s3error.ErrInternalError)
		return
	}
	dataReader := limitedDataReader
	// Build sha256sum if needed.
	inputSh256Sum := request.Request.Header.Get("X-Amz-Content-Sha256")
	sha256Writer := sha256.New()
	needCheckSha256 := false
	if inputSh256Sum != "" && inputSh256Sum != signature.UnsignedPayload {
		needCheckSha256 = true
		dataReader = io.TeeReader(limitedDataReader, sha256Writer)
	}
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

	// if read or send data failed, then close stream and return error
	if !eof {
		WriteErrorResponse(response, request, s3error.ErrInternalError)
		return
	}

	rsp := &s3.PutObjectResponse{}
	err = stream.RecvMsg(rsp)
	if HandleS3Error(response, request, err, rsp.GetErrorCode()) != nil {
		log.Errorf("stream receive message failed, err=%v, errCode=%d\n", err, rsp.GetErrorCode())
		return
	}

	// Check if sha256sum match.
	if needCheckSha256 {
		sha256Sum := hex.EncodeToString(sha256Writer.Sum(nil))
		if inputSh256Sum != sha256Sum {
			log.Errorln("sha256Sum:", sha256Sum, ", received:",
				request.Request.Header.Get("X-Amz-Content-Sha256"))
			WriteErrorResponse(response, request, s3error.ErrContentSHA256Mismatch)
			// Delete the object.
			input := s3.DeleteObjectInput{Bucket: bucketName, Key: objectKey}
			if len(rsp.VersionId) > 0 {
				input.VersioId = rsp.VersionId
			}
			ctx := common.InitCtxWithAuthInfo(request)
			s.s3Client.DeleteObject(ctx, &input)
		}
	}

	log.Infoln("object etag:", rsp.Md5)
	if rsp.Md5 != "" {
		response.AddHeader("ETag", "\""+rsp.Md5+"\"")
	}

	log.Info("PUT object successfully.")
	WriteSuccessResponse(response, nil)
}

func getSize(request *restful.Request, response *restful.Response) (int64, error) {
	// get content-length
	contentLenght := request.HeaderParameter(common.REQUEST_HEADER_CONTENT_LENGTH)
	size, err := strconv.ParseInt(contentLenght, 10, 64)
	if err != nil {
		log.Infof("parse contentLenght[%s] failed, err:%v\n", contentLenght, err)
		WriteErrorResponse(response, request, s3error.ErrMissingContentLength)
		return 0, err
	}

	log.Infof("object size is %v\n", size)

	if size > common.MaxObjectSize {
		log.Infof("invalid contentLenght:%s\n", contentLenght)
		errMsg := fmt.Sprintf("invalid contentLenght[%s], it should be less than %d and more than 0",
			contentLenght, common.MaxObjectSize)
		err := errors.New(errMsg)
		WriteErrorResponse(response, request, err)
		return size, err
	}

	return size, nil
}
