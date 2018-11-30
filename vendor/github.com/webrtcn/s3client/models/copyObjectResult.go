package models

import (
	"time"
)

//CopyObjectResult Copy Object Result
type CopyObjectResult struct {
	LastModified *time.Time
	Etag         string `xml:"ETag"`
}
