package constants

const (
	CACHE_NAME = "cachename"

	KEYDB_URL = "localhost:5380"

	KEYDB_PASSWORD = ""

	KEYDB_DB = 0
)

// type constants for cloud vendors
const (
	AWS_S3 = "AWS-S3"
)

var (
	AWS_VALID_REGIONS = []string{"us-east-1", "us-east-2", "us-west-1", "us-west-2", "af-south-1", "ap-east-1", "ap-south-2",
		"ap-southeast-3", "ap-south-1", "ap-northeast-3", "ap-northeast-2", "ap-southeast-1", "ap-southeast-2",
		"ap-northeast-1", "ca-central-1", "eu-central-1", "eu-west-1", "eu-west-2", "eu-south-1", "eu-west-3",
		"eu-south-2", "eu-north-1", "eu-central-2", "me-south-1", "me-central-1", "sa-east-1", "us-gov-east-1",
		"us-gov-west-1"}
)

const (
	ZERO = 0
)

const (
	LESS_THAN_OPERATOR          = "lt"
	LESS_THAN_EQUAL_OPERATOR    = "lte"
	EQUAL_OPERATOR              = "eq"
	GREATER_THAN_OPERATOR       = "gt"
	GREATER_THAN_EQUAL_OPERATOR = "gte"
)
