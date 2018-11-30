package models

import (
	"time"
)

//ListPartsResult list part result
type ListPartsResult struct {
	Bucket               string
	Key                  string
	UploadID             string `xml:"UploadId"`
	StorageClass         string
	PartNumberMarker     int
	NextPartNumberMarker int
	MaxParts             int
	IsTruncated          bool
	Owner                Owner
	Parts                Parts `xml:"Part"`
}

//Parts part list
type Parts []UpladPart

//UpladPart upload part
type UpladPart struct {
	LastModified time.Time
	PartNumber   int
	Etag         string `xml:"ETag"`
	Size         int64
}
