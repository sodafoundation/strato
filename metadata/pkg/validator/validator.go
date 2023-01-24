package validator

import (
	"github.com/opensds/multi-cloud/metadata/pkg/constants"
	pb "github.com/opensds/multi-cloud/metadata/proto"
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

	// validate region
	okie, err = isValidRegion(in.Region, in.Type)
	return okie, err
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
		// operator is valid

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
		// if cloudType is empty, then we wont check for validity
		return true, nil

	default:
		return false, &ValidationError{errMsg: "Not a valid cloud type"}
	}

	for _, validRegion := range validRegions {
		if validRegion == region {
			return true, nil
		}
	}
	return false, &ValidationError{errMsg: "Not a valid " + cloudType + " region"}
}
