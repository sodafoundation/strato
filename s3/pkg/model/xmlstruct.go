// Copyright 2019 The soda Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import "encoding/xml"

var Xmlns = "http://s3.amazonaws.com/doc/2006-03-01"

type CreateBucketConfiguration struct {
	Xmlns              string `xml:"xmlns,attr"`
	LocationConstraint string `xml:"LocationConstraint"`
	SSEOpts            SSEConfiguration
}

type Owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type Bucket struct {
	Name               string `xml:"Name"`
	CreateTime         string `xml:"CreateTime"`
	LocationConstraint string `xml:"LocationConstraint"`
	VersionOpts        VersioningConfiguration
	SSEOpts            SSEConfiguration
}

type ListAllMyBucketsResult struct {
	Xmlns   string   `xml:"xmlns,attr"`
	Owner   Owner    `xml:"Owner"`
	Buckets []Bucket `xml:"Buckets"`
}

type InitiateMultipartUploadResult struct {
	Xmlns    string `xml:"xmlns,attr"`
	Bucket   string `xml:"Bucket"`
	Key      string `xml:"Key"`
	UploadId string `xml:"UploadId"`
}

//PartNumber should be between 1 and 10000.
//Please refer to https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/qfacts.html
type UploadPartResult struct {
	Xmlns      string `xml:"xmlns,attr"`
	PartNumber int64  `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

// completedParts - is a collection satisfying sort.Interface.
type CompletedParts []Part

func (a CompletedParts) Len() int           { return len(a) }
func (a CompletedParts) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a CompletedParts) Less(i, j int) bool { return a[i].PartNumber < a[j].PartNumber }

type Part struct {
	PartNumber     int64  `xml:"PartNumber"`
	ETag           string `xml:"ETag"`
	Size           int64  `xml:"Size"`
	LastModifyTime int64  `xml:"LastModifyTime"`
}

type CompleteMultipartUpload struct {
	Xmlns string `xml:"xmlns,attr"`
	Parts []Part `xml:"Part"`
}

type CompleteMultipartUploadResult struct {
	Xmlns    string `xml:"xmlns,attr"`
	Location string `xml:"Location"`
	Bucket   string `xml:"Bucket"`
	Key      string `xml:"Key"`
	Size     int64  `xml:"Size"`
	ETag     string `xml:"ETag"`
}

type ListPartsOutput struct {
	Xmlns       string `xml:"xmlns,attr"`
	Bucket      string `xml:"Bucket"`
	Key         string `xml:"Key"`
	UploadId    string `xml:"UploadId"`
	MaxParts    int    `xml:"MaxParts"`
	IsTruncated bool   `xml:"IsTruncated"`
	Owner       Owner  `xml:"Owner"`
	Parts       []Part `xml:"Part"`
}

type LifecycleConfiguration struct {
	Rule []Rule `xml:"Rule"`
}

type SSEConfiguration struct {
	XMLName xml.Name `xml:"SSEConfiguration"`
	Text    string   `xml:",chardata"`
	SSE     struct {
		Text    string `xml:",chardata"`
		Enabled string `xml:"enabled"`
	} `xml:"SSE"`
	SSEKMS struct {
		Text                string `xml:",chardata"`
		Enabled             string `xml:"enabled"`
		DefaultKMSMasterKey string `xml:"DefaultKMSMasterKey"`
	} `xml:"SSE-KMS"`
}

type Rule struct {
	ID                             string                          `xml:"ID"`
	Filter                         *Filter                         `xml:"Filter,omitempty"`
	Status                         string                          `xml:"Status"`
	Transition                     []Transition                    `xml:"Transition,omitempty"`
	Expiration                     []Expiration                    `xml:"Expiration,omitempty"`
	AbortIncompleteMultipartUpload *AbortIncompleteMultipartUpload `xml:"AbortIncompleteMultipartUpload,omitempty"`
}

type Filter struct {
	Prefix string `xml:"Prefix"`
}

type Transition struct {
	Days         int32  `xml:"Days"`
	StorageClass string `xml:"StorageClass"`
	Backend      string `xml:"Backend"`
	TargetBucket string `xml:"TargetBucket"`
}

type Expiration struct {
	Days int32 `xml:"Days"`
	//Delete marker will be used in later release
	//ExpiredObjectDeleteMarker string   `xml:"ExpiredObjectDeleteMArker"`
}

type AbortIncompleteMultipartUpload struct {
	DaysAfterInitiation int32 `xml:"DaysAfterInitiation"`
}

type StorageClass struct {
	Name string `xml:"Name"`
	Tier int32  `xml:"Tier"`
}

type ListStorageClasses struct {
	Xmlns   string         `xml:"xmlns,attr"`
	Classes []StorageClass `xml:"Class"`
}

type VersioningConfiguration struct {
	XMLName xml.Name `xml:"VersioningConfiguration"`
	Status  string   `xml:"Status"`
}
