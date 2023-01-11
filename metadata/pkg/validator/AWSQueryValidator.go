package validator

import (
	log "github.com/micro/go-micro/v2/util/log"
	"github.com/opensds/multi-cloud/metadata/pkg/constants"
	pb "github.com/opensds/multi-cloud/metadata/proto"
)

type AWSQueryValidator struct {
}

func (v *AWSQueryValidator) isValidQuery(in *pb.ListMetadataRequest) (bool, error) {
	// validate aws region name
	okie, err := isValidRegion(in.Region)
	if !okie {
		log.Errorf("Validation for List MetaData Request Failed: %v", err)
		return false, err
	}

	return true, nil
}

func isValidRegion(region string) (bool, error) {
	validRegions := constants.AWS_VALID_REGIONS
	for _, validRegion := range validRegions {
		if validRegion == region {
			return true, nil
		}
	}
	return false, &ValidationError{errMsg: "Not a valid AWS region"}
}
