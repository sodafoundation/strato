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
	"net/url"
	"strconv"

	"github.com/emicklei/go-restful"
	"github.com/opensds/multi-cloud/api/pkg/common"
	"github.com/opensds/multi-cloud/api/pkg/s3/datatype"
	"github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/pkg/helper"
	"github.com/opensds/multi-cloud/s3/proto"
	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

// handle post object according to 'https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOST.html'
var ValidSuccessActionStatus = []string{"200", "201", "204"}

func (s *APIService) ObjectPost(request *restful.Request, response *restful.Response) {
	var err error
	// Here the parameter is the size of the form data that should
	// be loaded in memory, the remaining being put in temporary files.
	reader, err := request.Request.MultipartReader()
	if err != nil {
		log.Errorf("failed to get reader from post request, err: %v", err)
		WriteErrorResponse(response, request, s3error.ErrMalformedPOSTRequest)
		return
	}

	fileBody, formValues, err := extractHTTPFormValues(reader)
	if err != nil {
		log.Errorf("failed to extract form values, err: %v", err)
		WriteErrorResponse(response, request, s3error.ErrMalformedPOSTRequest)
		return
	}
	objectKey := formValues["Key"]
	if !isValidObjectName(objectKey) {
		log.Errorf("got invalid object key: %s", objectKey)
		WriteErrorResponse(response, request, s3error.ErrInvalidObjectName)
		return
	}

	bucketName := request.PathParameter(common.REQUEST_PATH_BUCKET_NAME)
	backendName := request.HeaderParameter(common.REQUEST_HEADER_STORAGE_CLASS)
	formValues["Bucket"] = bucketName

	// check if specific bucket exist
	ctx := common.InitCtxWithAuthInfo(request)
	bucketMeta, err := s.getBucketMeta(ctx, bucketName)
	if err != nil {
		log.Errorf("failed to get bucket meta. err: %v", err)
		WriteErrorResponse(response, request, err)
		return
	}
	location := bucketMeta.DefaultLocation
	if backendName != "" {
		// check if backend exist
		if s.isBackendExist(ctx, backendName) == false {
			log.Errorf("backend %s for bucket %s doesn't exist", backendName, bucketName)
			WriteErrorResponse(response, request, s3error.ErrGetBackendFailed)
			return
		}
		location = backendName
	}

	metadata := extractMetadataFromHeader(request)

	acl, err := getAclFromFormValues(formValues)
	if err != nil {
		log.Errorf("failed to get acl from from values, err: %v", err)
		WriteErrorResponse(response, request, s3error.ErrInvalidCannedAcl)
		return
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
		Size:       -1,
	}
	err = stream.SendMsg(&obj)
	if err != nil {
		log.Errorf("failed to call grpc PutObject(%v), err: %v", obj, err)
		WriteErrorResponse(response, request, s3error.ErrInternalError)
		return
	}
	for !eof {
		n, err := fileBody.Read(buf)
		if err != nil && err != io.EOF {
			log.Errorf("read error:%v", err)
			break
		}
		if err == io.EOF {
			log.Debugln("finished read")
			eof = true
		}
		err = stream.Send(&s3.PutDataStream{Data: buf[:n]})
		if err != nil {
			log.Infof("stream send error: %v", err)
			break
		}
		// make sure that the grpc server receives the EOF.
		if eof {
			stream.Send(&s3.PutDataStream{Data: buf[0:0]})
		}
	}

	// if read or send data failed, then close stream and return error
	if !eof {
		log.Errorf("failed to send data to put object.")
		WriteErrorResponse(response, request, s3error.ErrInternalError)
		return
	}

	result := &s3.PutObjectResponse{}
	err = stream.RecvMsg(result)
	if err != nil {
		log.Errorf("stream receive message failed:%v\n", err)
		WriteErrorResponse(response, request, s3error.ErrInternalError)
		return
	}

	log.Info("succeed to put object data for post object request.")
	if result.Md5 != "" {
		response.Header().Set("ETag", "\""+result.Md5+"\"")
	}

	var redirect string
	redirect, _ = formValues["Success_action_redirect"]
	if redirect == "" {
		redirect, _ = formValues["redirect"]
	}
	if redirect != "" {
		redirectUrl, err := url.Parse(redirect)
		if err == nil {
			redirectUrl.Query().Set("bucket", bucketName)
			redirectUrl.Query().Set("key", objectKey)
			redirectUrl.Query().Set("etag", result.Md5)
			http.Redirect(response, request.Request, redirectUrl.String(), http.StatusSeeOther)
			return
		}
		// If URL is Invalid, ignore the redirect field
	}

	var status string
	status, _ = formValues["Success_action_status"]
	if !helper.StringInSlice(status, ValidSuccessActionStatus) {
		status = "204"
	}

	statusCode, _ := strconv.Atoi(status)
	switch statusCode {
	case 200, 204:
		response.WriteHeader(statusCode)
	case 201:
		encodedSuccessResponse := EncodeResponse(datatype.PostResponse{
			// TODO the full accessable url is needed.
			Location: "/" + bucketName + "/" + objectKey,
			Bucket:   bucketName,
			Key:      objectKey,
			ETag:     result.Md5,
		})
		response.WriteHeader(201)
		response.Write(encodedSuccessResponse)
	}

}
