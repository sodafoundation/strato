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
	"net/url"
	"strings"
	"time"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/v2/client"
	log "github.com/sirupsen/logrus"

	"github.com/soda/multi-cloud/api/pkg/common"
	apiutils "github.com/soda/multi-cloud/api/pkg/utils"
	. "github.com/soda/multi-cloud/s3/error"
	"github.com/soda/multi-cloud/s3/pkg/meta/types"
	"github.com/soda/multi-cloud/s3/pkg/utils"
	pb "github.com/soda/multi-cloud/s3/proto"
)

func getTierFromHeader(request *restful.Request) (types.StorageClass, error) {
	storageClassStr := request.HeaderParameter(common.REQUEST_HEADER_STORAGE_CLASS)
	if storageClassStr != "" {
		return types.MatchStorageClassIndex(storageClassStr)
	} else {
		// If you don't specify this header, STANDARD will be used
		return utils.Tier1, nil
	}
}

// ObjectCopy copy object from http header x-amz-copy-source
func (s *APIService) ObjectCopy(request *restful.Request, response *restful.Response) {
	log.Infof("received request: Copy object")

	targetBucketName := request.PathParameter(common.REQUEST_PATH_BUCKET_NAME)
	targetObjectName := request.PathParameter(common.REQUEST_PATH_OBJECT_KEY)
	//backendName := request.HeaderParameter(common.REQUEST_HEADER_STORAGE_CLASS)
	log.Infof("received request: Copy object, objectkey=%s, bucketName=%s\n:",
		targetObjectName, targetBucketName)

	// copy source is of form: /bucket-name/object-name?versionId=xxxxxx
	copySource := request.HeaderParameter(common.REQUEST_HEADER_COPY_SOURCE)
	if copySource == "" {
		WriteErrorResponse(response, request, ErrInvalidCopySource)
		return
	}
	// Skip the first element if it is '/', split the rest.
	if strings.HasPrefix(copySource, "/") {
		copySource = copySource[1:]
	}
	splits := strings.SplitN(copySource, "/", 2)

	// Save sourceBucket and sourceObject extracted from url Path.
	var err error
	var sourceBucketName, sourceObjectName, sourceVersion string
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
		WriteErrorResponse(response, request, ErrInvalidCopySource)
		return
	}

	splits = strings.SplitN(sourceObjectName, "?", 2)
	if len(splits) == 2 {
		sourceObjectName = splits[0]
		if !strings.HasPrefix(splits[1], "versionId=") {
			WriteErrorResponse(response, request, ErrInvalidCopySource)
			return
		}
		sourceVersion = strings.TrimPrefix(splits[1], "versionId=")
	}

	// X-Amz-Copy-Source should be URL-encoded
	sourceBucketName, err = url.QueryUnescape(sourceBucketName)
	if err != nil {
		WriteErrorResponse(response, request, ErrInvalidCopySource)
		return
	}
	sourceObjectName, err = url.QueryUnescape(sourceObjectName)
	if err != nil {
		WriteErrorResponse(response, request, ErrInvalidCopySource)
		return
	}

	log.Infoln("sourceBucketName:", sourceBucketName, " sourceObjectName:", sourceObjectName, " sourceVersion:", sourceVersion)

	ctx := common.InitCtxWithAuthInfo(request)
	sourceObject, _, _, err := s.getObjectMeta(ctx, sourceBucketName, sourceObjectName, "", false)
	if err != nil {
		log.Errorln("unable to fetch object info. err:", err)
		WriteErrorResponse(response, request, err)
		return
	}

	// Verify before x-amz-copy-source preconditions before continuing with CopyObject.
	if err = checkObjectPreconditions(response.ResponseWriter, request.Request, sourceObject); err != nil {
		WriteErrorResponse(response, request, err)
		return
	}

	//TODO: In a versioning-enabled bucket, you cannot change the storage class of a specific version of an object. When you copy it, Amazon S3 gives it a new version ID.
	storClass, err := getTierFromHeader(request)
	if err != nil {
		WriteErrorResponse(response, request, err)
		return
	}

	var updateMetaData bool
	newMetadata := make(map[string]string)
	if request.HeaderParameter("X-Amz-Metadata-Directive") == "REPLACE" {
		updateMetaData = true
		newMetadata = extractMetadataFromHeader(request.Request.Header)
	}

	// if source == dest and X-Amz-Metadata-Directive == REPLACE, only update the meta;
	if sourceBucketName == targetBucketName && sourceObjectName == targetObjectName {
		if !updateMetaData {
			WriteErrorResponse(response, request, ErrInvalidCopyDest)
			return
		}

		// only update metadata
		log.Infoln("only update metadata.")
		targetObject := sourceObject

		//update custom attrs from headers
		if c, ok := newMetadata["Content-Type"]; ok {
			targetObject.ContentType = c
		} else {
			targetObject.ContentType = sourceObject.ContentType
		}
		targetObject.CustomAttributes = newMetadata
		targetObject.Tier = int32(storClass)

		result, err := s.s3Client.UpdateObjectMeta(ctx, targetObject)
		if err != nil {
			log.Errorf("unable to update object meta for %v", targetObject.ObjectId)
			WriteErrorResponse(response, request, err)
			return
		}
		copyObjRes := GenerateCopyObjectResponse(result.Md5, time.Unix(result.LastModified, 0))
		encodedSuccessResponse := EncodeResponse(copyObjRes)
		// write headers
		if result.Md5 != "" {
			response.ResponseWriter.Header()["ETag"] = []string{"\"" + result.Md5 + "\""}
		}
		if sourceVersion != "" {
			response.AddHeader("x-amz-copy-source-version-id", sourceVersion)
		}
		if result.VersionId != "" {
			response.AddHeader("x-amz-version-id", result.VersionId)
		}

		log.Info("Update object meta successfully.")
		// write success response.
		WriteSuccessResponse(response, encodedSuccessResponse)
		return
	}

	/// maximum Upload size for object in a single CopyObject operation.
	if isMaxObjectSize(sourceObject.Size) {
		WriteErrorResponseWithResource(response, request, ErrEntityTooLarge, copySource)
		return
	}

	acl, err := getAclFromHeader(request)
	if err != nil {
		WriteErrorResponse(response, request, err)
		return
	}

	log.Infoln("srcBucket:", sourceBucketName, " srcObject:", sourceObjectName,
		" targetBucket:", targetBucketName, " targetObject:", targetObjectName)
	tmoutSec := apiutils.GetTimeoutSec(sourceObject.Size)
	opt := client.WithRequestTimeout(time.Duration(tmoutSec) * time.Second)
	result, err := s.s3Client.CopyObject(ctx, &pb.CopyObjectRequest{
		SrcBucketName:    sourceBucketName,
		TargetBucketName: targetBucketName,
		SrcObjectName:    sourceObjectName,
		TargetObjectName: targetObjectName,
		Acl:              &pb.Acl{CannedAcl: acl.CannedAcl},
		CustomAttributes: newMetadata,
	}, opt)
	if HandleS3Error(response, request, err, result.GetErrorCode()) != nil {
		log.Errorf("unable to copy object, err=%v, errCode=%v\n", err, result.GetErrorCode())
		return
	}

	copyObjRes := GenerateCopyObjectResponse(result.Md5, time.Unix(result.LastModified, 0))
	encodedSuccessResponse := EncodeResponse(copyObjRes)
	// write headers
	if result.Md5 != "" {
		response.ResponseWriter.Header()["ETag"] = []string{"\"" + result.Md5 + "\""}
	}

	// write success response.
	WriteSuccessResponse(response, encodedSuccessResponse)
	log.Info("COPY object successfully.")
}
