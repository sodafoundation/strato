/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package s3

import (
	"encoding/xml"
	"net/http"
	"path"
	"strconv"

	"github.com/emicklei/go-restful"
	. "github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/pkg/helper"
)

const (
	timeFormatAMZ = "2006-01-02T15:04:05.000Z" // Reply date format
)


// WriteSuccessResponse write success headers and response if any.
func WriteSuccessResponse(response *restful.Response, data []byte) {
	if data == nil {
		response.WriteHeader(http.StatusOK)
		return
	}

	response.AddHeader("Content-Length", strconv.Itoa(len(data)))
	response.WriteHeader(http.StatusOK)
	response.Write(data)
	response.Flush()
}

// writeErrorResponse write error headers
// w http.ResponseWriter, r *http.Request
func WriteErrorResponse(response *restful.Response, request *restful.Request, err error) {
	WriteErrorResponseHeaders(response, err)
	WriteErrorResponseNoHeader(response, request, err, request.Request.URL.Path)
}

func WriteErrorResponseHeaders(response *restful.Response, err error) {
	var status int
	apiErrorCode, ok := err.(S3Error)
	if ok {
		status = apiErrorCode.HttpStatusCode()
	} else {
		status = http.StatusInternalServerError
	}

	response.WriteHeader(status)
}

func WriteErrorResponseNoHeader(response *restful.Response, request *restful.Request, err error, resource string) {
	// HEAD should have no body, do not attempt to write to it
	if request.Request.Method == "HEAD" {
		return
	}

	// Generate error response.
	errorResponse := ApiErrorResponse{}
	apiErrorCode, ok := err.(S3Error)
	if ok {
		errorResponse.AwsErrorCode = apiErrorCode.AwsErrorCode()
		errorResponse.Message = apiErrorCode.Description()
	} else {
		errorResponse.AwsErrorCode = "InternalError"
		errorResponse.Message = "We encountered an internal error, please try again."
	}
	errorResponse.Resource = resource
	errorResponse.HostId = helper.CONFIG.InstanceId

	encodedErrorResponse := EncodeResponse(errorResponse)

	response.Write(encodedErrorResponse)
	response.ResponseWriter.(http.Flusher).Flush()
}

// getLocation get URL location.
func GetLocation(r *http.Request) string {
	return path.Clean(r.URL.Path) // Clean any trailing slashes.
}

// writeSuccessNoContent write success headers with http status 204
func WriteSuccessNoContent(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNoContent)
}

func WriteApiErrorResponse(response *restful.Response, request *restful.Request, status int, awsErrCode, message string) {
	// write header
	response.WriteHeader(status)

	// HEAD should have no body, do not attempt to write to it
	if request.Request.Method == "HEAD" {
		return
	}

	errorResponse := ApiErrorResponse{}
	errorResponse.AwsErrorCode = awsErrCode
	errorResponse.Message = message
	errorResponse.Resource = request.Request.URL.Path
	errorResponse.HostId = helper.CONFIG.InstanceId

	encodedErrorResponse := EncodeResponse(errorResponse)

	response.Write(encodedErrorResponse)
	response.ResponseWriter.(http.Flusher).Flush()
}

// APIErrorResponse - error response format
type ApiErrorResponse struct {
	XMLName      xml.Name `xml:"Error" json:"-"`
	AwsErrorCode string   `xml:"Code"`
	Message      string
	Key          string
	BucketName   string
	Resource     string
	RequestId    string
	HostId       string
}
