package models

//InitiatedMultipartUploadsResult Initiated Multipart Uploads Result model
type InitiatedMultipartUploadsResult struct {
	Bucket   string // The bucket that will receive the object contents.
	Key      string // The key specified by the key request parameter (if any).
	UploadID string `xml:"UploadId"` //The ID specified by the upload-id request parameter identifying the multipart upload (if any).
}
