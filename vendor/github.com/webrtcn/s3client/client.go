package s3client

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"

	"github.com/webrtcn/s3client/models"
)

//Client s3 Client
type Client struct {
	endPoint        string
	accessKey       string
	secretAccessKey string
}

//NewClient init s3 client
func NewClient(endPoint, accessKey, secretAccessKey string) *Client {
	return &Client{
		endPoint:        endPoint,
		secretAccessKey: secretAccessKey,
		accessKey:       accessKey,
	}
}

//NewBucket new bucket service
func (c *Client) NewBucket() *Bucket {
	return &Bucket{
		client: c,
	}
}

func (c *Client) do(req *request, entity interface{}, bodyFunc func(resp *http.Response)) error {
	httpClient := http.DefaultClient
	reqURL, err := url.Parse(c.endPoint + req.getQueryString())
	if err != nil {
		return err
	}
	sign := req.sign(c.accessKey, c.secretAccessKey)
	request, err := http.NewRequest(string(req.method), reqURL.String(), req.playload)
	request.Header.Set("Authorization", sign)
	request.Header.Set("Host", request.Host)
	for k, v := range req.headers {
		request.Header.Set(k, v[0])
	}
	if v, ok := req.headers["Content-Length"]; ok {
		request.ContentLength, _ = strconv.ParseInt(v[0], 10, 64)
	}
	resp, err := httpClient.Do(request)
	if err != nil {
		return err
	}
	switch resp.StatusCode {
	case 200, 206:
		if bodyFunc != nil {
			bodyFunc(resp)
		} else {
			if entity == nil {
				return nil
			}
			defer resp.Body.Close()
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				return err
			}
			if len(body) > 0 {
				err := xml.Unmarshal(body, entity)
				if err != nil {
					return err
				}
			}
		}
	case 400, 403, 404, 405, 408, 409, 411, 412, 416, 422, 500:
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		if len(body) > 0 {
			er := &models.ErrorResult{}
			err = xml.Unmarshal(body, er)
			if err != nil {
				return err
			}
			return errors.New(er.Code)
		}
		return fmt.Errorf("Request error, status code: %s", strconv.Itoa(resp.StatusCode))
	}
	return nil
}
