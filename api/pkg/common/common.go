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
	"fmt"
	"strconv"
	"strings"

	"github.com/emicklei/go-restful"
	log "github.com/sirupsen/logrus"
)

const (
	MaxPaginationLimit      = 1000
	DefaultPaginationLimit  = MaxPaginationLimit
	DefaultPaginationOffset = 0
	SortDirectionAsc        = "asc"
	SortDirectionDesc       = "desc"
)

const (
	KLimit        = "limit"
	KOffset       = "offset"
	KSort         = "sort"
	KLastModified = "lastmodified"
	KObjKey       = "objkey"
	KStorageTier  = "tier"
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
