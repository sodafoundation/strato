package validator

import (
	"github.com/opensds/multi-cloud/metadata/pkg/constants"
	pb "github.com/opensds/multi-cloud/metadata/proto"
)

type GeneralQueryValidator struct {
}

func (v *GeneralQueryValidator) isValidQuery(in *pb.ListMetadataRequest) (bool, error) {
	// TODO: validate backend name if present is valid according to backend naming conventions

	// validate size of object and operator is valid
	okie, err := isSizeParamsValid(in.SizeOfBucketInBytes, in.BucketSizeOperator)
	if !okie {
		return okie, err
	}

	// validate size of object and operator is valid
	okie, err = isSizeParamsValid(in.SizeOfObjectInBytes, in.ObjectSizeOperator)

	return okie, err
}

func isSizeParamsValid(sizeInBytes int32, operator string) (bool, error) {
	switch operator {
	case constants.EQUAL_OPERATOR:

	case constants.LESS_THAN_EQUAL_OPERATOR:

	case constants.LESS_THAN_OPERATOR:

	case constants.GREATER_THAN_EQUAL_OPERATOR:

	case constants.GREATER_THAN_OPERATOR:
		// operator is valid
		break
	default:
		return false, &ValidationError{errMsg: "Operator for size should be  lte, gt, eq, gte or lt"}
	}

	if sizeInBytes < constants.ZERO {
		return false, &ValidationError{errMsg: "Size should be always positive."}
	}

	return true, nil
}
