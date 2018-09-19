package utils

import "github.com/opensds/go-panda/s3/proto"

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
	Obj *s3.Object
}