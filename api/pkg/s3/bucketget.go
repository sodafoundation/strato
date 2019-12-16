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
	"time"
	"unicode/utf8"

	"github.com/emicklei/go-restful"
	"github.com/opensds/multi-cloud/api/pkg/common"
	"github.com/opensds/multi-cloud/api/pkg/s3/datatype"
	"github.com/opensds/multi-cloud/api/pkg/utils/constants"
	. "github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/pkg/helper"
	"github.com/opensds/multi-cloud/s3/pkg/utils"
	"github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

func (s *APIService) BucketGet(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	log.Infof("Received request for bucket details: %s\n", bucketName)

	var err error
	req, err := parseListObjectsQuery(request.Request.URL.Query())
	if err != nil {
		WriteErrorResponse(response, request, err)
		return
	}
	req.Bucket = bucketName

	ctx := common.InitCtxWithAuthInfo(request)
	lsRsp, err := s.s3Client.ListObjects(ctx, &req)
	if HandleS3Error(response, request, err, lsRsp.GetErrorCode()) != nil {
		log.Errorf("get bucket[%s] failed, err=%v, errCode=%d\n", bucketName, err, lsRsp.ErrorCode)
		return
	}

	rsp := CreateListObjectsResponse(bucketName, &req, lsRsp)
	log.Debugf("rsp:%+v\n", rsp)
	// Write success response.
	response.WriteEntity(rsp)

	return
}

func parseListObjectsQuery(query url.Values) (request s3.ListObjectsRequest, err error) {
	if query.Get("list-type") == constants.ListObjectsType2Str {
		request.Version = constants.ListObjectsType2Int
		request.ContinuationToken = query.Get("continuation-token")
		request.StartAfter = query.Get("start-after")
		if !utf8.ValidString(request.StartAfter) {
			err = ErrNonUTF8Encode
			return
		}
		request.FetchOwner = helper.Ternary(query.Get("fetch-owner") == "true",
			true, false).(bool)
	} else {
		request.Version = constants.ListObjectsType1Int
		request.Marker = query.Get("marker")
		if !utf8.ValidString(request.Marker) {
			err = ErrNonUTF8Encode
			return
		}
	}
	request.Delimiter = query.Get("delimiter")
	if !utf8.ValidString(request.Delimiter) {
		err = ErrNonUTF8Encode
		return
	}
	request.EncodingType = query.Get("encoding-type")
	if request.EncodingType != "" && request.EncodingType != "url" {
		err = ErrInvalidEncodingType
		return
	}
	if query.Get("max-keys") == "" {
		request.MaxKeys = utils.MaxObjectList
	} else {
		var maxKey int
		maxKey, err = strconv.Atoi(query.Get("max-keys"))
		if err != nil {
			log.Errorf("parsing max-keys error:%v\n", err)
			return request, ErrInvalidMaxKeys
		}
		request.MaxKeys = int32(maxKey)
		if request.MaxKeys > utils.MaxObjectList || request.MaxKeys < 1 {
			err = ErrInvalidMaxKeys
			return
		}
	}
	request.Prefix = query.Get("prefix")
	if !utf8.ValidString(request.Prefix) {
		err = ErrNonUTF8Encode
		return
	}

	request.KeyMarker = query.Get("key-marker")
	if !utf8.ValidString(request.KeyMarker) {
		err = ErrNonUTF8Encode
		return
	}
	request.VersionIdMarker = query.Get("version-id-marker")
	if !utf8.ValidString(request.VersionIdMarker) {
		err = ErrNonUTF8Encode
		return
	}

	log.Infof("request:%+v\n", request)
	return
}

// this function refers to GenerateListObjectsResponse in api-response.go from Minio Cloud Storage.
func CreateListObjectsResponse(bucketName string, request *s3.ListObjectsRequest,
	listRsp *s3.ListObjectsResponse) (response datatype.ListObjectsResponse) {
	for _, o := range listRsp.Objects {
		obj := datatype.Object{
			Key:          o.ObjectKey,
			LastModified: time.Unix(o.LastModified, 0).In(time.Local).Format(timeFormatAMZ),
			ETag:         o.Etag,
			Size:         o.Size,
			StorageClass: o.StorageClass,
			Location:     o.Location,
			Tier:         o.Tier,
		}
		if request.EncodingType != "" { // only support "url" encoding for now
			obj.Key = url.QueryEscape(obj.Key)
		}
		if request.FetchOwner {
			obj.Owner.ID = o.TenantId //TODO: DisplayName
		}
		response.Contents = append(response.Contents, obj)
	}

	var prefixes []datatype.CommonPrefix
	for _, prefix := range listRsp.Prefixes {
		item := datatype.CommonPrefix{
			Prefix: prefix,
		}
		prefixes = append(prefixes, item)
	}
	response.CommonPrefixes = prefixes

	response.Delimiter = request.Delimiter
	response.EncodingType = request.EncodingType
	response.IsTruncated = listRsp.IsTruncated
	response.MaxKeys = int(request.MaxKeys)
	response.Prefix = request.Prefix
	response.BucketName = bucketName

	if request.Version == constants.ListObjectsType2Int {
		response.KeyCount = len(response.Contents)
		response.ContinuationToken = request.ContinuationToken
		response.NextContinuationToken = listRsp.NextMarker
		response.StartAfter = request.StartAfter
	} else { // version 1
		response.Marker = request.Marker
		response.NextMarker = listRsp.NextMarker
	}

	if request.EncodingType != "" {
		response.Delimiter = url.QueryEscape(response.Delimiter)
		response.Prefix = url.QueryEscape(response.Prefix)
		response.StartAfter = url.QueryEscape(response.StartAfter)
		response.Marker = url.QueryEscape(response.Marker)
	}

	return
}

func (s *APIService) HeadBucket(request *restful.Request, response *restful.Response) {
	bucketName := request.PathParameter("bucketName")
	log.Infof("Received request for head bucket: %s\n", bucketName)

	var err error

	ctx := common.InitCtxWithAuthInfo(request)
	_, err = s.getBucketMeta(ctx, bucketName)
	if err != nil {
		log.Errorf("get bucket[%s] failed, err=%v\n", bucketName, err)
		WriteErrorResponse(response, request, err)
		return
	}

	log.Debugln("head bucket succeed")
	WriteSuccessResponse(response, nil)
}
