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

package common

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/v2/metadata"
	log "github.com/sirupsen/logrus"

	c "github.com/opensds/multi-cloud/api/pkg/context"
)

const (
	MaxPaginationLimit      = 1000
	DefaultPaginationLimit  = MaxPaginationLimit
	DefaultPaginationOffset = 0
	MaxObjectSize           = 5 * 1024 * 1024 * 1024 // 5GB
	SortDirectionAsc        = "asc"
	SortDirectionDesc       = "desc"
	DefaultAdminTenantId    = "94b280022d0c4401bcf3b0ea85870519"
)

const (
	KLimit        = "limit"
	KOffset       = "offset"
	KSort         = "sort"
	KLastModified = "lastmodified"
	KObjKey       = "objkey"
	KStorageTier  = "tier"
	KPrefix       = "prefix"
	KMarker       = "marker"
	KDelimiter    = "delimiter"
	KVerMarker    = "verMarker"
)

const (
	CTX_KEY_TENANT_ID   = "Tenantid"
	CTX_KEY_USER_ID     = "Userid"
	CTX_KEY_IS_ADMIN    = "Isadmin"
	CTX_VAL_TRUE        = "true"
	CTX_REPRE_TENANT    = "Representedtenantid"
	CTX_KEY_OBJECT_KEY  = "ObjectKey"
	CTX_KEY_BUCKET_NAME = "BucketName"
	CTX_KEY_SIZE        = "ObjectSize"
	CTX_KEY_LOCATION    = "Location"
)

const (
	REQUEST_PATH_BUCKET_NAME         = "bucketName"
	REQUEST_PATH_OBJECT_KEY          = "objectKey"
	REQUEST_PATH_BACKEND_ID          = "backendId"
	REQUEST_HEADER_CONTENT_LENGTH    = "Content-Length"
	REQUEST_HEADER_BACKEND           = "x-amz-meta-backend"
	REQUEST_HEADER_STORAGE_CLASS     = "x-amz-storage-class"
	REQUEST_HEADER_COPY_SOURCE       = "X-Amz-Copy-Source"
	REQUEST_HEADER_COPY_SOURCE_RANGE = "X-Amz-Copy-Source-Range"
	REQUEST_HEADER_ACL               = "X-Amz-Acl"
	REQUEST_HEADER_CONTENT_MD5       = "Content-Md5"
	REQUEST_HEADER_CONTENT_TYPE      = "Content-Type"
	REQUEST_HEADER_SSE_KEY           = "x-amz-server-side-encryption"
	REQUEST_HEADER_SSE_VALUE_AES256  = "AES256"
	REQUEST_HEADER_ARCHIVE           = "X-Amz-Storage-Class"
)

const (
	REQUEST_FORM_KEY    = "Key"
	REQUEST_FORM_BUCKET = "Bucket"
)

func GetPaginationParam(request *restful.Request) (int32, int32, error) {
	limit := int32(DefaultPaginationLimit)
	offset := int32(DefaultPaginationOffset)

	if request.QueryParameter(KLimit) != "" {
		limitVal, err := strconv.Atoi(request.QueryParameter("limit"))
		if err != nil {
			log.Errorf("limit is invalid: %v", err)
			return limit, offset, err
		}
		if limit > int32(limitVal) {
			limit = int32(limitVal)
		}
	}

	if request.QueryParameter(KOffset) != "" {
		offsetVal, err := strconv.Atoi(request.QueryParameter("offset"))
		if err != nil {
			log.Errorf("offset is invalid: %v", err)
			return limit, offset, err
		}
		offset = int32(offsetVal)
	}

	if limit < 0 || offset < 0 {
		msg := fmt.Sprintf("Invalid pagination parameter, limit = %d and offset = %d.", limit, offset)
		log.Errorf(msg)
		return limit, offset, errors.New(msg)
	}

	return limit, offset, nil
}

// An example of sort key parameter will be like this: sort=key1:asc,key2:desc
func GetSortParam(request *restful.Request) (sortKeys []string, sortDirs []string, err error) {
	sortStr := request.QueryParameter(KSort)
	if sortStr != "" {
		return
	}

	sortStr = strings.TrimSpace(sortStr)
	for _, sort := range strings.Split(sortStr, ",") {
		parts := strings.Split(sort, ":")
		switch {
		case len(parts) > 2:
			err = fmt.Errorf("invalid sort value %s", sort)
			return
		case len(parts) == 1:
			parts = append(parts, SortDirectionAsc)
		}
		sortKeys = append(sortKeys, parts[0])
		sortDirs = append(sortDirs, parts[1])
	}
	return
}

func GetFilter(request *restful.Request, filterOpts []string) (map[string]string, error) {

	filter := make(map[string]string)
	for _, opt := range filterOpts {
		v := request.QueryParameter(opt)
		if v == "" {
			continue
		}
		filter[opt] = v
	}
	return filter, nil
}

func GetFilterPathParams(request *restful.Request, filterOpts []string) (map[string]string, error) {

	filter := make(map[string]string)
	for _, opt := range filterOpts {
		v := request.PathParameter(opt)
		if v == "" {
			continue
		}
		filter[opt] = v
	}
	return filter, nil
}

func InitCtxWithAuthInfo(request *restful.Request) context.Context {
	actx := request.Attribute(c.KContext).(*c.Context)
	ctx := metadata.NewContext(context.Background(), map[string]string{
		CTX_KEY_USER_ID:   actx.UserId,
		CTX_KEY_TENANT_ID: actx.TenantId,
		CTX_KEY_IS_ADMIN:  strconv.FormatBool(actx.IsAdmin),
	})

	return ctx
}

// TODO: Can be optimized with above function InitCtxWithAuthInfo()
func InitCtxWithAuthInfoNonAdmin(request *restful.Request) context.Context {
	actx := request.Attribute(c.KContext).(*c.Context)
	var ctx context.Context
	if actx.IsAdmin {
		ctx = metadata.NewContext(context.Background(), map[string]string{
			CTX_KEY_USER_ID:   actx.UserId,
			CTX_KEY_TENANT_ID: actx.TenantId,
			CTX_KEY_IS_ADMIN:  strconv.FormatBool(true),
		})
	} else {
		ctx = metadata.NewContext(context.Background(), map[string]string{
			CTX_KEY_TENANT_ID: request.PathParameter("tenantId"),
			CTX_KEY_IS_ADMIN:  strconv.FormatBool(false),
		})
	}

	return ctx
}

func GetAdminContext() context.Context {
	var ctx context.Context
	ctx = metadata.NewContext(context.Background(), map[string]string{
		CTX_KEY_TENANT_ID: DefaultAdminTenantId,
		CTX_KEY_IS_ADMIN:  strconv.FormatBool(true),
	})
	return ctx
}

func GetOwner(request *restful.Request) (ownerId string) {
	actx := request.Attribute(c.KContext).(*c.Context)
	return actx.TenantId
}

func InitCtxWithVal(request *restful.Request, md map[string]string) context.Context {
	actx := request.Attribute(c.KContext).(*c.Context)
	md[CTX_KEY_USER_ID] = actx.UserId
	md[CTX_KEY_TENANT_ID] = actx.TenantId
	md[CTX_KEY_IS_ADMIN] = strconv.FormatBool(actx.IsAdmin)

	return metadata.NewContext(context.Background(), md)
}

func GetSizeRequestParamAsInt64(request *restful.Request, param string) (int64, error) {

	var val int
	if request.QueryParameter(param) != "" {
		val, err := strconv.Atoi(request.QueryParameter(param))
		if err != nil {
			log.Errorf("Param Value is invalid: %v", err)
			return int64(val), err
		}

		return int64(val), nil

	}
	return int64(val), nil

}
