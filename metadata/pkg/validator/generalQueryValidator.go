package validator

import (
	pb "github.com/opensds/multi-cloud/metadata/proto"
)

type GeneralQueryValidator struct {
}

func (v *GeneralQueryValidator) isValidQuery(in *pb.ListMetadataRequest) (bool, error) {
	// TODO: validate backend name if present is valid according to backend naming conventions

	// TODO: validate size of bucket is valid

	// TODO: validate size of object is valid

	return true, nil
}
