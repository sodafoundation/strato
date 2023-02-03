// Copyright 2023 The SODA Authors.
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

package query_manager

import (
	"github.com/opensds/multi-cloud/metadata/pkg/constants"
	pb "github.com/opensds/multi-cloud/metadata/proto"
	"strings"
)

type ValidationError struct {
	errMsg string
}

func (e *ValidationError) Error() string {
	return e.errMsg
}

func ValidateInput(in *pb.ListMetadataRequest) (okie bool, err error) {
	return isValidQuery(in)
}

func isValidQuery(in *pb.ListMetadataRequest) (bool, error) {
	// TODO: validate backend name if present is valid according to backend naming conventions

	in.Type = strings.ToLower(in.Type)
	// validate size of object and operator is valid
	okie, err := isSizeParamsValid(in.SizeOfBucketInBytes, in.BucketSizeOperator)
	if !okie {
		return okie, err
	}

	// validate size of object and operator is valid
	okie, err = isSizeParamsValid(in.SizeOfObjectInBytes, in.ObjectSizeOperator)
	if !okie {
		return okie, err
	}

	okie, err = isSortParamValid(in.SortOrder)
	if !okie {
		return okie, err
	}
	// validate region
	okie, err = isValidRegion(in.Region, in.Type)
	return okie, err
}

func isSortParamValid(sortOrder string) (bool, error) {
	sortOrder = strings.ToLower(sortOrder)
	if sortOrder == "" || sortOrder == constants.ASC || sortOrder == constants.DESC {
		return true, nil
	}
	return false, &ValidationError{errMsg: "Invalid sort order"}
}

func isSizeParamsValid(sizeInBytes int64, operator string) (bool, error) {

	if sizeInBytes == 0 && operator == "" {
		return true, nil
	}

	switch operator {
	case constants.EQUAL_OPERATOR:

	case constants.LESS_THAN_EQUAL_OPERATOR:

	case constants.LESS_THAN_OPERATOR:

	case constants.GREATER_THAN_EQUAL_OPERATOR:

	case constants.GREATER_THAN_OPERATOR:

	default:
		return false, &ValidationError{errMsg: "Operator for size should be  lte, gt, eq, gte or lt"}
	}

	if sizeInBytes < constants.ZERO {
		return false, &ValidationError{errMsg: "Size should be always positive."}
	}

	return true, nil
}

func isValidRegion(region string, cloudType string) (bool, error) {
	var validRegions []string

	switch cloudType {
	case constants.AWS_S3:
		validRegions = constants.AWS_VALID_REGIONS

	case "":
		// if cloudType is empty, then we won't check for validity
		return true, nil

	default:
		return false, &ValidationError{errMsg: "Not a valid cloud type"}
	}

	if region == "" {
		return true, nil
	}

	for _, validRegion := range validRegions {
		if validRegion == region {
			return true, nil
		}
	}
	return false, &ValidationError{errMsg: "Not a valid " + cloudType + " region"}
}
