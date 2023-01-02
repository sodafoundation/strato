package validator

import (
	pb "github.com/opensds/multi-cloud/metadata/proto"
)

type ValidationError struct {
	errMsg string
}

type QueryValidator interface {
	isValidQuery(in *pb.ListMetadataRequest) (bool, error)
}

func (e *ValidationError) Error() string {
	return e.errMsg
}

func ValidateInput(in *pb.ListMetadataRequest) (okie bool, err error) {
	okie, err = isValidQueryOptions(in)
	if !okie {
		return
	}

	switch in.Type {
	case "":
		validator := &GeneralQueryValidator{}
		return validator.isValidQuery(in)

		// TODO: Get Type values from pravin
	case "AMAZON-AWS":
		var validator QueryValidator = &GeneralQueryValidator{}
		okie, err = validator.isValidQuery(in)
		if !okie {
			return
		}
		validator = &AWSQueryValidator{}
		return validator.isValidQuery(in)
	default:
		validator := &GeneralQueryValidator{}
		return validator.isValidQuery(in)
	}
}
