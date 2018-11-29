package s3client

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

type method string

const (
	get     = method("GET")
	post    = method("POST")
	put     = method("PUT")
	delete  = method("DELETE")
	head    = method("HEAD")
	empty   = ""
	newLine = "\n"
)

var s3ParamsToSign = map[string]bool{
	"acl":                          true,
	"location":                     true,
	"logging":                      true,
	"notification":                 true,
	"partNumber":                   true,
	"policy":                       true,
	"requestPayment":               true,
	"torrent":                      true,
	"uploadId":                     true,
	"uploads":                      true,
	"versionId":                    true,
	"versioning":                   true,
	"versions":                     true,
	"response-content-type":        true,
	"response-content-language":    true,
	"response-expires":             true,
	"response-cache-control":       true,
	"response-content-disposition": true,
	"response-content-encoding":    true,
	"website":                      true,
	"delete":                       true,
}

type suffix struct {
	key   string
	value string
	flag  bool
}

type request struct {
	method   method
	bucket   string
	object   string
	headers  http.Header
	playload io.ReadCloser
	suffixs  []suffix
}

func (req *request) getCanonicalizedResource() string {
	var suffix string
	slx := len(req.suffixs)
	if slx > 0 {
		for _, item := range req.suffixs {
			if s3ParamsToSign[item.key] == false {
				continue
			}
			if item.flag {
				if len(suffix) > 0 {
					suffix = fmt.Sprintf("%s&%s", suffix, item.key)
				} else {
					suffix = item.key
				}
			} else {
				if len(suffix) > 0 {
					suffix = fmt.Sprintf("%s&%s=%s", suffix, item.key, item.value)
				} else {
					suffix = fmt.Sprintf("%s=%s", item.key, item.value)
				}
			}
		}
	}
	bl := len(req.bucket)
	ol := len(req.object)
	sl := len(suffix)
	if bl == 0 {
		return "/"
	}
	if ol == 0 {
		if sl == 0 {
			if slx == 0 {
				return fmt.Sprintf("/%s/", req.bucket)
			}
			return fmt.Sprintf("/%s", req.bucket)
		}
		return fmt.Sprintf("/%s?%s", req.bucket, suffix)
	}
	if sl == 0 {
		return fmt.Sprintf("/%s/%s", req.bucket, req.object)
	}
	return fmt.Sprintf("/%s/%s?%s", req.bucket, req.object, suffix)
}

func (req *request) getQueryString() string {
	var suffix string
	sl := len(req.suffixs)
	if sl > 0 {
		for _, item := range req.suffixs {
			if item.flag {
				if len(suffix) > 0 {
					suffix = fmt.Sprintf("%s&%s", suffix, item.key)
				} else {
					suffix = item.key
				}
			} else {
				if len(suffix) > 0 {
					suffix = fmt.Sprintf("%s&%s=%s", suffix, item.key, item.value)
				} else {
					suffix = fmt.Sprintf("%s=%s", item.key, item.value)
				}
			}
		}
	}
	bl := len(req.bucket)
	ol := len(req.object)
	if bl == 0 {
		return "/"
	}
	if ol == 0 {
		if sl == 0 {
			return fmt.Sprintf("/%s/", req.bucket)
		}
		return fmt.Sprintf("/%s?%s", req.bucket, suffix)
	}
	if sl == 0 {
		return fmt.Sprintf("/%s/%s", req.bucket, req.object)
	}
	return fmt.Sprintf("/%s/%s?%s", req.bucket, req.object, suffix)
}

func (req *request) getContentMD5() string {
	for k, v := range req.headers {
		k = strings.ToLower(k)
		if k == "content-md5" {
			return v[0]
		}
	}
	return empty
}

func (req *request) getContentType() string {
	for k, v := range req.headers {
		k = strings.ToLower(k)
		if k == "content-type" {
			return v[0]
		}
	}
	return empty
}

func (req *request) getCanonicalizedAmzHeaders() string {
	var items KeyValuePairList
	for k, v := range req.headers {
		k = strings.ToLower(k)
		if strings.HasPrefix(k, "x-amz-") {
			values := strings.Join(v, ",")
			items = append(items, KeyValuePair{k, fmt.Sprintf("%s:%s", k, values)})
		}
	}
	var xamz string
	if len(items) > 0 {
		sort.Sort(items)
		xamz = strings.Join(items.ToArray(), newLine) + newLine
	}
	return xamz
}

func (req *request) stringToSign() string {
	date := time.Now().In(time.UTC).Format(time.RFC1123)
	if req.headers == nil {
		req.headers = http.Header{}
	}
	req.headers["Date"] = []string{date}
	if req.method == empty {
		req.method = get
	}
	md5 := req.getContentMD5()
	ct := req.getContentType()
	headers := req.getCanonicalizedAmzHeaders()
	resource := req.getCanonicalizedResource()
	verb := string(req.method)
	return fmt.Sprintf("%s\n%s\n%s\n%s\n%s%s", verb, md5, ct, date, headers, resource)
}

func (req *request) signature(accessKey, secretAccessKey string) string {
	hash := hmac.New(sha1.New, []byte(secretAccessKey))
	signString := req.stringToSign()
	hash.Write([]byte(signString))
	signature := make([]byte, base64.StdEncoding.EncodedLen(hash.Size()))
	base64.StdEncoding.Encode(signature, hash.Sum(nil))
	return string(signature)
}

func (req *request) sign(accessKey, secretAccessKey string) string {
	signature := req.signature(accessKey, secretAccessKey)
	return fmt.Sprintf("AWS %s:%s", accessKey, signature)
}
