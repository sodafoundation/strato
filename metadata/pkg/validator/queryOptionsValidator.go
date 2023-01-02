package validator

import (
	log "github.com/micro/go-micro/v2/util/log"
	pb "github.com/opensds/multi-cloud/metadata/proto"
)

func isValidQueryOptions(req *pb.ListMetadataRequest) (bool, error) {

	// Check if the no of records expected by the user is positive
	if req.Limit < 1 {
		log.Errorf("Validation for List MetaData Request Failed: %v", "Limit must be greater than zero")
		return false, &ValidationError{errMsg: "Limit must be greater than zero"}
	}

	// Ensure that the index used for denoting the start position is valid
	if req.Offset < 0 {
		log.Errorf("Validation for List MetaData Request Failed: %v", "Offset must be greater than or equal to zero")
		return false, &ValidationError{errMsg: "Offset must be greater than or equal to zero"}
	}

	return true, nil
}
