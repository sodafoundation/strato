package validator

import (
	pb "github.com/opensds/multi-cloud/metadata/proto"
)

func isValidQueryOptions(req *pb.ListMetadataRequest) (bool, error) {
	return true, nil
}
