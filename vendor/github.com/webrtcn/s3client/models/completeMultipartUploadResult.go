package models

//CompleteMultipartUploadResult Complete Multipart Upload Result
type CompleteMultipartUploadResult struct {
	Bucket string
	Key    string
	Etag   string `xml:"ETag"`
}
