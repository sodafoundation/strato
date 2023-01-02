package validator

import (
	pb "github.com/opensds/multi-cloud/metadata/proto"
)

type ValidationError struct {
	errMsg string
}

func (e *ValidationError) Error() string {
	return e.errMsg
}

func ValidateInput(in *pb.ListMetadataRequest) (okie bool, err error) {
	okie, err = isValidQueryOptions(in)
	if !okie {
		return
	}
	return
}
