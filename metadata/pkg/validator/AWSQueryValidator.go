package validator

import (
	pb "github.com/opensds/multi-cloud/metadata/proto"
)

type AWSQueryValidator struct {
}

func (v *AWSQueryValidator) isValidQuery(in *pb.ListMetadataRequest) (bool, error) {
	// TODO: validate bucket name if present

	// TODO: validate object name if present

	// TODO: validate region if present
	return true, nil
}
