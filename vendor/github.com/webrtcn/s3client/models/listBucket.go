package models

import(
	"encoding/xml"
	"time"
)

type BucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Owner Owner
	Buckets BucketDetail
} 

type BucketDetail struct {
	Bucket []BucketItem
}

type BucketItem struct {
	Name string
	CreationDate time.Time
}