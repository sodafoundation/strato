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

	// validate aws region name
	okie, err = isValidRegion(in.Region)
	if !okie {
		log.Errorf("Validation for List MetaData Request Failed: %v", err)
	}
	return true, nil
}

func isValidRegion(region string) (bool, error) {
	validRegions := []string{"us-east-1", "us-east-2", "us-west-1", "us-west-2", "af-south-1", "ap-east-1", "ap-south-2",
		"ap-southeast-3", "ap-south-1", "ap-northeast-3", "ap-northeast-2", "ap-southeast-1", "ap-southeast-2",
		"ap-northeast-1", "ca-central-1", "eu-central-1", "eu-west-1", "eu-west-2", "eu-south-1", "eu-west-3",
		"eu-south-2", "eu-north-1", "eu-central-2", "me-south-1", "me-central-1", "sa-east-1", "us-gov-east-1",
		"us-gov-west-1"}
	for _, validRegion := range validRegions {
		if validRegion == region {
			return true, nil
		}
	}
	return false, &ValidationError{errMsg: "Not a valid region"}
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
