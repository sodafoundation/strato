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
	"github.com/emicklei/go-restful"
	log "github.com/sirupsen/logrus"

	"github.com/soda/multi-cloud/api/pkg/common"
	pb "github.com/soda/multi-cloud/s3/proto"

	. "github.com/soda/multi-cloud/s3/error"
)

func (s *APIService) MultiPartUploadInit(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter(common.REQUEST_PATH_BUCKET_NAME)
	objectKey := request.PathParameter(common.REQUEST_PATH_OBJECT_KEY)
	backendName := request.HeaderParameter(common.REQUEST_HEADER_BACKEND)

	log.Infof("received request: multipart init, objectkey=%s, bucketName=%s\n:",
		objectKey, bucketName)

	if !isValidObjectName(objectKey) {
		log.Errorf("object name is not valid.")
		WriteErrorResponse(response, request, ErrInvalidObjectName)
		return
	}

	acl, err := getAclFromHeader(request)
	if err != nil {
		log.Errorf("failed to get acl from http header, err:%s", err)
		WriteErrorResponse(response, request, err)
		return
	}

	// Save metadata.
	attr := extractMetadataFromHeader(request.Request.Header)

	tier, err := getTierFromHeader(request)
	if err != nil {
		log.Errorf("failed to get storage class from http header. err:%s", err)
		WriteErrorResponse(response, request, err)
		return
	}

	ctx := common.InitCtxWithAuthInfo(request)
	location := ""
	if backendName != "" {
		// check if backend exist
		if s.isBackendExist(ctx, backendName) == false {
			WriteErrorResponse(response, request, ErrGetBackendFailed)
			return
		}
		location = backendName
	}

	result, err := s.s3Client.InitMultipartUpload(ctx, &pb.InitMultiPartRequest{
		BucketName: bucketName,
		ObjectKey:  objectKey,
		Acl:        &pb.Acl{CannedAcl: acl.CannedAcl},
		Tier:       int32(tier),
		Location:   location,
		Attrs:      attr})
	if HandleS3Error(response, request, err, result.GetErrorCode()) != nil {
		log.Errorln("unable to init multipart. err:, errcode:", err, result.GetErrorCode())
		return
	}

	data := GenerateInitiateMultipartUploadResponse(bucketName, objectKey, result.UploadID)
	encodedSuccessResponse := EncodeResponse(data)
	// write success response.
	WriteSuccessResponse(response, encodedSuccessResponse)

	log.Infof("Init multipart upload[bucketName=%s, objectKey=%s] successfully.\n",
		bucketName, objectKey)
}
