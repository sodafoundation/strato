package models

import (
	"encoding/xml"
	"time"
)

//ListMultipartUploadsResult List Multipart UploadsR esult
type ListMultipartUploadsResult struct {
	XMLName            xml.Name `xml:"ListMultipartUploadsResult"`
	Bucket             string
	NextKeyMarker      string
	NextUploadIDMarker string `xml:"NextUploadIdMarker"`
	MaxUploads         int
	IsTruncated        bool
	Upload             []Upload
}

//Upload Upload
type Upload struct {
	Key          string
	UploadID     string `xml:"UploadId"`
	Initiator    Initiator
	Owner        Owner
	StorageClass string
	Initiated    time.Time
}

//Initiator Initiator
type Initiator struct {
	UserID      string `xml:"ID"`
	DisplayName string
}
