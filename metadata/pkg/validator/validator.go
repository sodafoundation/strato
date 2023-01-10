package validator

import (
	"github.com/opensds/multi-cloud/metadata/pkg/constants"
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
	switch in.Type {
	case "":
		validator := &GeneralQueryValidator{}
		return validator.isValidQuery(in)

	case constants.AWS_S3:
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
