package s3client

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"

	"github.com/webrtcn/s3client/models"
)

//Uploads Multipart Uploads
type Uploads struct {
	bucketName string
	objectName string
	client     *Client
}

//CompletePart CompletePart
type CompletePart struct {
	PartNumber int
	Etag       string `xml:"ETag"`
}

//CompleteUpload CompleteUpload
type CompleteUpload struct {
	XMLName xml.Name      `xml:"CompleteMultipartUpload"`
	Parts   CompleteParts `xml:"Part"`
}

//CompleteParts CompleteParts
type CompleteParts []CompletePart

func (p CompleteParts) Len() int           { return len(p) }
func (p CompleteParts) Less(i, j int) bool { return p[i].PartNumber < p[j].PartNumber }
func (p CompleteParts) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

//CreateUploadsOption Create uploadsOption
type CreateUploadsOption struct {
	ContentMD5  string
	ContentType string
	Perm        models.ACL
	Metadatas   map[string]string
}

//Initiate uploads first
func (u *Uploads) Initiate(option *CreateUploadsOption) (*models.InitiatedMultipartUploadsResult, error) {
	headers := http.Header{}
	if option != nil {
		if len(option.ContentMD5) > 0 {
			headers.Add("Content-MD5", option.ContentMD5)
		}
		if len(option.ContentType) > 0 {
			headers.Add("Content-Type", option.ContentType)
		}
		if len(option.Perm) > 0 {
			headers.Add("x-amz-acl", string(option.Perm))
		}
		if len(option.Metadatas) > 0 {
			for k, v := range option.Metadatas {
				headers.Add(fmt.Sprintf("x-amz-meta-%s", k), v)
			}
		}
	}
	req := &request{
		method:  post,
		bucket:  u.bucketName,
		object:  u.objectName,
		headers: headers,
		suffixs: []suffix{
			suffix{
				key:  "uploads",
				flag: true,
			},
		},
	}
	entity := &models.InitiatedMultipartUploadsResult{}
	err := u.client.do(req, entity, nil)
	return entity, err
}

//UploadPart Upload part
func (u *Uploads) UploadPart(partNumber int, uploadID, contentMd5, contentType string, contentLength int64, body io.ReadCloser) (*CompletePart, error) {
	if partNumber < 1 {
		return nil, errors.New("pageNumber be more than 0")
	}
	req := &request{
		method: put,
		bucket: u.bucketName,
		object: u.objectName,
		headers: http.Header{
			"Content-Length": {strconv.FormatInt(contentLength, 10)},
			"Content-MD5":    {contentMd5},
			"Content-Type":   {contentType},
		},
		suffixs: []suffix{
			suffix{
				key:   "partNumber",
				value: strconv.Itoa(partNumber),
			},
			suffix{
				key:   "uploadId",
				value: uploadID,
			},
		},
		playload: body,
	}
	part := &CompletePart{}
	err := u.client.do(req, nil, func(resp *http.Response) {
		if resp.StatusCode == 200 {
			etag := resp.Header.Get("Etag")
			if len(etag) > 0 {
				part.Etag = etag
				part.PartNumber = partNumber
			}
		}
	})
	return part, err
}

//ListPart list all parts
func (u *Uploads) ListPart(uploadID string) (*models.ListPartsResult, error) {
	req := &request{
		method: get,
		bucket: u.bucketName,
		object: u.objectName,
		suffixs: []suffix{
			suffix{
				key:   "uploadId",
				value: uploadID,
				flag:  false,
			},
		},
	}
	value := &models.ListPartsResult{}
	err := u.client.do(req, value, nil)
	return value, err
}

//Complete part upload
func (u *Uploads) Complete(uploadID string, completeParts CompleteParts) (*models.CompleteMultipartUploadResult, error) {
	sort.Sort(completeParts)
	result := &CompleteUpload{
		Parts: completeParts,
	}
	data, _ := xml.Marshal(result)
	body := ioutil.NopCloser(bytes.NewReader(data))
	contentLength := int64(len(data))
	req := &request{
		method: post,
		bucket: u.bucketName,
		object: u.objectName,
		headers: http.Header{
			"Content-Length": {strconv.FormatInt(contentLength, 10)},
		},
		suffixs: []suffix{
			suffix{
				key:   "uploadId",
				value: uploadID,
				flag:  false,
			},
		},
		playload: body,
	}
	value := &models.CompleteMultipartUploadResult{}
	err := u.client.do(req, value, nil)
	return value, err
}

//RemoveUploads delete uploads with uploadid
func (u *Uploads) RemoveUploads(uploadID string) error {
	req := &request{
		method: delete,
		bucket: u.bucketName,
		object: u.objectName,
		suffixs: []suffix{
			suffix{
				key:   "uploadId",
				value: uploadID,
				flag:  false,
			},
		},
	}
	return u.client.do(req, nil, nil)
}
