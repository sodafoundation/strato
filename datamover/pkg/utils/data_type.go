package utils

type LocationInfo struct {
	StorType string //aws-s3,azure-blob,hw-obs,etc.
	Region string
	EndPoint string
	BucketName string //remote bucket name
	Access string
	Security string
	BakendId string
}

type SourceOject struct {
	StorType string //aws-s3,azure-blob,hw-obs,etc.
	ObjKey string
	Backend string
	Size int64
}