package validator

import (
	"regexp"
	"strings"

	log "github.com/micro/go-micro/v2/util/log"
	pb "github.com/opensds/multi-cloud/metadata/proto"
)

type AWSQueryValidator struct {
}

func (v *AWSQueryValidator) isValidQuery(in *pb.ListMetadataRequest) (bool, error) {
	// validate bucket name if present
	okie, err := isValidAWSBucketName(in.BucketName)
	if !okie {
		log.Errorf("Validation for List MetaData Request Failed: %v", err)
	}
	// TODO: validate object name if present

	// TODO: validate region if present
	return true, nil
}

//* This function checks if the bucket name is valid according to the AWS bucket naming rules found in
//* https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
func isValidAWSBucketName(bucketName string) (bool, error) {
	bucketNameLen := len(bucketName)

	//* if the bucket name is not provided , terminate the validation
	if bucketName == "" {
		return true, nil
	}

	// TODO: replace with constants
	if bucketNameLen < 3 || bucketNameLen > 63 {
		return false, &ValidationError{errMsg: "Valid AWS bucket name length must be between 3 and 63"}
	}

	if matched, _ := regexp.MatchString("[a-z0-9][-.a-z0-9]+[a-z0-9]", bucketName); !matched {
		return false, &ValidationError{errMsg: "Valid AWS bucket name must begin and end with lowercase alphanumeric characters and can contain only lower case alphanumeric characters , hyphens and dots"}
	}

	if strings.Contains(bucketName, "..") {
		return false, &ValidationError{errMsg: "Valid AWS bucket name must not contain two consecutive periods"}
	}

	if matched, _ := regexp.MatchString("([0-9]{1,3}[.]){3}[0-9]{1,3}", bucketName); matched {
		return false, &ValidationError{errMsg: "Valid AWS bucket name must not match an IP address"}
	}

	if strings.HasPrefix(bucketName, "xn--") {
		return false, &ValidationError{errMsg: "Valid AWS bucket name must not begin with xn--"}
	}

	if strings.HasSuffix(bucketName, "-s3alias") {
		return false, &ValidationError{errMsg: "Valid AWS bucket name must not end with -s3alias"}
	}

	return true, nil
}
