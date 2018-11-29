package s3client

import (

	//"fmt"
	"bytes"
	"encoding/xml"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/Click2Cloud/s3client/models"
)

//Bucket Bucket
type Bucket struct {
	client *Client
}

//NewObject create object service
func (b *Bucket) NewObject(bucketName string) *Object {
	return &Object{
		client:     b.client,
		bucketName: bucketName,
	}
}

//List all buckets
func (b *Bucket) List() (*models.BucketsResult, error) {
	req := &request{
		method: get,
	}
	buckets := &models.BucketsResult{}
	err := b.client.do(req, buckets, nil)
	return buckets, err
}

//Create bucket with given name
func (b *Bucket) Create(name string, perm models.ACL) error {
	req := &request{
		method: put,
		bucket: name,
		headers: http.Header{
			"x-amz-acl": {string(perm)},
		},
	}
	err := b.client.do(req, nil, nil)
	return err
}

//Remove bucket with given name
func (b *Bucket) Remove(name string) error {
	req := &request{
		method: delete,
		bucket: name,
	}
	err := b.client.do(req, nil, nil)
	return err
}

/*
	Get get bucket
		prefix:		Only returns objects that contain the specified prefix.
		delimiter:	The delimiter between the prefix and the rest of the object name.
		marker:	 	A beginning index for the list of objects returned.
		maxKeys:	The maximum number of keys to return. Default is 1000.
*/
func (b *Bucket) Get(bucketName, prefix, delimiter, marker string, maxKeys int) (*models.GetBucketResponse, error) {
	suffixs := []suffix{
		suffix{
			key:   "max-keys",
			value: strconv.Itoa(maxKeys),
			flag:  false,
		},
		suffix{
			key:   "marker",
			value: marker,
			flag:  false,
		},
		suffix{
			key:   "delimiter",
			value: delimiter,
			flag:  false,
		},
		suffix{
			key:   "prefix",
			value: prefix,
			flag:  false,
		},
	}
	req := &request{
		method:  get,
		bucket:  bucketName,
		suffixs: suffixs,
	}
	resp := &models.GetBucketResponse{}
	err := b.client.do(req, resp, nil)
	return resp, err
}

//GetLocation get location of the bucket with given name
func (b *Bucket) GetLocation(name string) (string, error) {
	suffixs := []suffix{
		suffix{
			key:  "location",
			flag: true,
		},
	}
	req := &request{
		method:  get,
		bucket:  name,
		suffixs: suffixs,
	}
	value := &models.LocationConstraint{}
	err := b.client.do(req, value, nil)
	return value.LocationConstraint, err
}

//GetACL get bucket acl
func (b *Bucket) GetACL(name string) (*models.AccessControlPolicy, error) {
	suffixs := []suffix{
		suffix{
			key:  "acl",
			flag: true,
		},
	}
	req := &request{
		method:  get,
		bucket:  name,
		suffixs: suffixs,
	}
	value := &models.AccessControlPolicy{}
	err := b.client.do(req, value, nil)
	return value, err
}

//SetACL set bucket acl
func (b *Bucket) SetACL(name string, perm models.ACL) error {
	suffixs := []suffix{
		suffix{
			key:  "acl",
			flag: true,
		},
	}
	req := &request{
		method:  put,
		bucket:  name,
		suffixs: suffixs,
		headers: http.Header{
			"x-amz-acl": {string(perm)},
		},
	}
	err := b.client.do(req, nil, nil)
	return err
}

/*
	ListUploads list all uploads under the bucket
	name bucket name
	prefix Returns in-progress uploads whose keys contains the specified prefix.
	delimiter The delimiter between the prefix and the rest of the object name.
	keyMarker The beginning marker for the list of uploads.
	uploadIDMarker Ignored if key-marker isn’t specified. Specifies the ID of first upload to list in lexicographical order at or following the ID.
	maxKeys The maximum number of in-progress uploads. The default is 1000.
	maxUploads The maximum number of multipart uploads. The range from 1-1000. The default is 1000.
*/
func (b *Bucket) ListUploads(name, prefix, delimiter, keyMarker, uploadIDMarker string, maxKeys, maxUploads int) (*models.ListMultipartUploadsResult, error) {
	suffixs := []suffix{
		suffix{
			key:  "uploads",
			flag: true,
		},
		suffix{
			key:   "prefix",
			value: prefix,
			flag:  false,
		},
		suffix{
			key:   "delimiter",
			value: delimiter,
			flag:  false,
		},
		suffix{
			key:   "key-marker",
			value: keyMarker,
			flag:  false,
		},
		suffix{
			key:   "max-keys",
			value: strconv.Itoa(maxKeys),
			flag:  false,
		},
		suffix{
			key:   "max-uploads",
			value: strconv.Itoa(maxUploads),
			flag:  false,
		},
		suffix{
			key:   "upload-id-marker",
			value: uploadIDMarker,
			flag:  false,
		},
	}
	req := &request{
		method:  get,
		bucket:  name,
		suffixs: suffixs,
	}
	value := &models.ListMultipartUploadsResult{}
	err := b.client.do(req, value, nil)
	return value, err
}

//SetVersioning set bucket version enabled.
func (b *Bucket) SetVersioning(name string, enable bool) error {
	status := models.VersioningSuspended
	if enable {
		status = models.VersioningEnabled
	}
	v := models.Versioning{
		Status: status,
	}
	data, _ := xml.Marshal(&v)
	body := ioutil.NopCloser(bytes.NewReader(data))
	contentLength := strconv.Itoa(len(data))
	suffixs := []suffix{
		suffix{
			key:  "versioning",
			flag: true,
		},
	}
	req := &request{
		method:   put,
		bucket:   name,
		suffixs:  suffixs,
		playload: body,
		headers: http.Header{
			"Content-Length": {contentLength},
		},
	}
	err := b.client.do(req, nil, nil)
	return err
}
