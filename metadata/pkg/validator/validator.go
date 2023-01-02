package validator

import (
	pb "github.com/opensds/multi-cloud/metadata/proto"
)

type ValidationError struct {
	errMsg string
}

func ValidateInput(in *pb.ListMetadataRequest) (bool, error) {
	okie, err := isValidQueryOptions(in)
	if !okie {
		return false, err
	}
	return true, nil
}

func (e *ValidationError) Error() string {
	return e.errMsg
}
