package validator

import (
	pb "github.com/opensds/multi-cloud/metadata/proto"
)

func isValidQueryOptions(req *pb.ListMetadataRequest) (bool, error) {

	// Check if the no of records expected by the user is positive
	if req.Limit < 1 {
		return false, &ValidationError{errMsg: "Limit must be greater than zero"}
	}

	// Ensure that the index used for denoting the start position is valid
	if req.Offset < 0 {
		return false, &ValidationError{errMsg: "Offset must be greater than or equal to zero"}
	}

	return true, nil
}
