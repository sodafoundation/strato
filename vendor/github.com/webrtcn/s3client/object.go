package s3client

import (
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/webrtcn/s3client/models"
)

//Object Object
type Object struct {
	client     *Client
	bucketName string
}

//NewUploads Initiated multipart uploads
func (o *Object) NewUploads(objName string) *Uploads {
	uploader := &Uploads{
		client:     o.client,
		bucketName: o.bucketName,
		objectName: objName,
	}
	return uploader
}

//GetObjectOption download file optioin
type GetObjectOption struct {
	Range             *Range //The range of the object to retrieve.
	IfModifiedSince   string //Gets only if modified since the timestamp.
	IfUnmodifiedSince string //Gets only if not modified since the timestamp.
	IfMatchTag        string //Gets only if object ETag matches ETag.
	IfNotMatchTag     string //Gets only if object ETag matches ETag.
}

//Range The range of the object to retrieve.
type Range struct {
	Begin int64
	End   int64
}

//CopyObjectOption Copy Object Option
type CopyObjectOption struct {
	Perm                  *models.ACL //A canned ACL.
	CopyIfModifiedSince   string      //Copies only if modified since the timestamp.
	CopyIfUnmodifiedSince string      //Copies only if unmodified since the timestamp.
	CopyIfMatchTag        string      //Copies only if object ETag matches ETag.
	CopyIfNotMatchTag     string      //Copies only if object ETag doesn’t match.
}

//Create upload object to bucket
func (o *Object) Create(name, contentMd5, contentType string, contentLength int64, body io.ReadCloser, perm models.ACL) (err error) {
	header := http.Header{
		"x-amz-acl":      {string(perm)},
		"Content-Length": {strconv.FormatInt(contentLength, 10)},
		"Content-MD5":    {contentMd5},
		"Content-Type":   {contentType},
	}
	req := &request{
		method:   put,
		bucket:   o.bucketName,
		object:   name,
		headers:  header,
		playload: body,
	}
	err = o.client.do(req, nil, nil)
	return
}

//Get get object from bucket with name
func (o *Object) Get(name string, option *GetObjectOption) (*http.Response, error) {
	header := http.Header{}
	if option != nil {
		if option.Range != nil {
			var value string
			if option.Range.End == 0 {
				value = fmt.Sprintf("bytes=%d-", option.Range.Begin)
			} else {
				value = fmt.Sprintf("bytes=%d-%d", option.Range.Begin, option.Range.End)
			}
			header.Add("Range", value)
		}
		if len(option.IfModifiedSince) > 0 {
			header.Add("If-Modified-Since", option.IfModifiedSince)
		}
		if len(option.IfUnmodifiedSince) > 0 {
			header.Add("If-Unmodified-Since", option.IfUnmodifiedSince)
		}
		if len(option.IfMatchTag) > 0 {
			header.Add("If-Match", option.IfMatchTag)
		}
		if len(option.IfNotMatchTag) > 0 {
			header.Add("If-None-Match", option.IfNotMatchTag)
		}
	}
	req := &request{
		method:  get,
		bucket:  o.bucketName,
		object:  name,
		headers: header,
	}
	var response *http.Response
	err := o.client.do(req, nil, func(e *http.Response) {
		response = e
	})
	return response, err
}

//Copy object
func (o *Object) Copy(sourceBucket, sourceObject, destObject string, option *CopyObjectOption) (*models.CopyObjectResult, error) {
	req := &request{
		method: put,
		bucket: o.bucketName,
		object: destObject,
		headers: http.Header{
			"x-amz-copy-source": {fmt.Sprintf("%s/%s", sourceBucket, sourceObject)},
		},
	}
	if option != nil {
		if option.Perm != nil {
			req.headers.Add("x-amz-acl", string(*option.Perm))
		}
		if len(option.CopyIfMatchTag) > 0 {
			req.headers.Add("x-amz-copy-if-match", option.CopyIfMatchTag)
		}
		if len(option.CopyIfModifiedSince) > 0 {
			req.headers.Add("x-amz-copy-if-modified-since", option.CopyIfModifiedSince)
		}
		if len(option.CopyIfNotMatchTag) > 0 {
			req.headers.Add("x-amz-copy-if-none-match", option.CopyIfNotMatchTag)
		}
		if len(option.CopyIfUnmodifiedSince) > 0 {
			req.headers.Add("x-amz-copy-if-unmodified-since", option.CopyIfUnmodifiedSince)
		}
	}
	value := &models.CopyObjectResult{}
	err := o.client.do(req, value, nil)
	return value, err
}

//Remove object from bucket
func (o *Object) Remove(name string) error {
	req := &request{
		method: delete,
		bucket: o.bucketName,
		object: name,
	}
	return o.client.do(req, nil, nil)
}

//GetHeader get object header information
func (o *Object) GetHeader(name string, option *GetObjectOption) (*http.Response, error) {
	header := http.Header{}
	if option != nil {
		if option.Range != nil {
			var value string
			if option.Range.End == 0 {
				value = fmt.Sprintf("bytes=%d-", option.Range.Begin)
			} else {
				value = fmt.Sprintf("bytes=%d-%d", option.Range.Begin, option.Range.End)
			}
			header.Add("Range", value)
		}
		if len(option.IfModifiedSince) > 0 {
			header.Add("If-Modified-Since", option.IfModifiedSince)
		}
		if len(option.IfUnmodifiedSince) > 0 {
			header.Add("If-Unmodified-Since", option.IfUnmodifiedSince)
		}
		if len(option.IfMatchTag) > 0 {
			header.Add("If-Match", option.IfMatchTag)
		}
		if len(option.IfNotMatchTag) > 0 {
			header.Add("If-None-Match", option.IfNotMatchTag)
		}
	}
	req := &request{
		method:  head,
		bucket:  o.bucketName,
		object:  name,
		headers: header,
	}
	var response *http.Response
	err := o.client.do(req, nil, func(e *http.Response) {
		response = e
	})
	return response, err
}

//GetACL get acl of the object
func (o *Object) GetACL(name string) (*models.AccessControlPolicy, error) {
	req := &request{
		method: get,
		bucket: o.bucketName,
		object: name,
		suffixs: []suffix{
			suffix{
				key:  "acl",
				flag: true,
			},
		},
	}
	policy := &models.AccessControlPolicy{}
	err := o.client.do(req, policy, nil)
	return policy, err
}

//SetACL set acl of the object with header
func (o *Object) SetACL(name string, perm models.ACL) error {
	req := &request{
		method: put,
		bucket: o.bucketName,
		object: name,
		suffixs: []suffix{
			suffix{
				key:  "acl",
				flag: true,
			},
		},
		headers: http.Header{
			"x-amz-acl": {string(perm)},
		},
	}
	return o.client.do(req, nil, nil)
}
