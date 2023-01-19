package constants

const (
	CACHE_NAME     = "cachename"
	KEYDB_URL      = "localhost:5380"
	KEYDB_PASSWORD = ""
	KEYDB_DB       = 0
)

// type constants for cloud vendors
const (
	AWS_S3 = "AWS-S3"
)

// valid region for AWS
var (
	AWS_VALID_REGIONS = []string{"us-east-1", "us-east-2", "us-west-1", "us-west-2", "af-south-1", "ap-east-1", "ap-south-2",
		"ap-southeast-3", "ap-south-1", "ap-northeast-3", "ap-northeast-2", "ap-southeast-1", "ap-southeast-2",
		"ap-northeast-1", "ca-central-1", "eu-central-1", "eu-west-1", "eu-west-2", "eu-south-1", "eu-west-3",
		"eu-south-2", "eu-north-1", "eu-central-2", "me-south-1", "me-central-1", "sa-east-1", "us-gov-east-1",
		"us-gov-west-1"}
)

const (
	ZERO          = 0
	DOLLAR_SYMBOL = "$"
	DOLLAR_DOLLAR = "$$"
	DOT           = "."
)

const (
	LESS_THAN_OPERATOR          = "lt"
	LESS_THAN_EQUAL_OPERATOR    = "lte"
	EQUAL_OPERATOR              = "eq"
	GREATER_THAN_OPERATOR       = "gt"
	GREATER_THAN_EQUAL_OPERATOR = "gte"
)

// mongodb constants
const (
	MATCH_AGG_OP   = "$match"
	UNWIND_AGG_OP  = "$unwind"
	PATH           = "path"
	SORT_AGG_OP    = "$sort"
	FILTER_AGG_OP  = "$filter"
	PROJECT_AGG_OP = "$project"
	COND_OPERATOR  = "cond"
	INPUT          = "input"
	AS             = "as"
	//comparioson operators
	DOLLAR_EQUAL_OPERATOR = "$eq"
	//logical_operators
	OR_OPERATOR            = "$or"
	AND_OPERATOR           = "$and"
	MAP_AGG_OPERATOR       = "$map"
	PROJECT_AGG_OPERATOR   = "$project"
	INCLUDE_FIELD          = 1
	IN                     = "in"
	ELEMMATCH_AGG_OPERATOR = "$elemMatch"
	SORT_AGG_OPERATOR      = "$sort"

	ASCENDING_ORDER = 1
)

const (
	ID           = "_id"
	BACKEND_NAME = "backendName"
	TYPE         = "type"
	REGION       = "region"
	BUCKETS      = "buckets"
	BUCKET       = "bucket"
	NAME         = "name"
	//* bucket.+ actual field name
	BUCKET_NAME        = "bucket.name"
	BUCKETS_NAME       = "buckets.name"
	BUCKET_TOTAL_SIZE  = "bucket.totalSize"
	BUCKETS_TOTAL_SIZE = "buckets.totalSize"
	TOTAL_SIZE         = "totalSize"
	OBJECTS            = "objects"
	OBJECT             = "object"
	//* object.+ actual field name
	OBJECT_SIZE       = "object.size"
	OBJECT_NAME       = "object.name"
	OBJECTS_SIZE      = "objects.size"
	OBJECTS_NAME      = "objects.name"
	CREATION_DATE     = "creationDate"
	ACCESS            = "access"
	NUMBER_OF_OBJECTS = "numberOfObjects"
	TAGS              = "tags"
)
