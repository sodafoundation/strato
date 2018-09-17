package s3

var Xmlns = "http://s3.amazonaws.com/doc/2006-03-01"

type CreateBucketConfiguration struct {
	Xmlns              string `xml:"xmlns,attr"`
	LocationConstraint string `xml:"LocationConstraint"`
}

type Owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type Bucket struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type ListAllMyBucketsResult struct {
	Xmlns   string   `xml:"xmlns,attr"`
	Owner   Owner    `xml:"Owner"`
	Buckets []Bucket `xml:"Buckets"`
}
