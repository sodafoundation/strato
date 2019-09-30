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

	"github.com/emicklei/go-restful"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
)

const (
	timeFormatAMZ = "2006-01-02T15:04:05.000Z" // Reply date format
)

// writeErrorResponse write error headers
// w http.ResponseWriter, r *http.Request
func WriteErrorResponse(request *restful.Request, response *restful.Response, err error) {
	WriteErrorResponseHeaders(response, err)
	WriteErrorResponseNoHeader(response, request, err, request.Request.URL.Path)
}

func WriteErrorResponseHeaders(response *restful.Response, err error) {
	var status int
	apiErrorCode, ok := err.(ApiError)
	if ok {
		status = apiErrorCode.HttpStatusCode()
	} else {
		status = http.StatusInternalServerError
	}
	helper.Logger.Println(20, "Response status code:", status, "err:", err)

	//ResponseRecorder
	//w.(*ResponseRecorder).status = status

	response.WriteHeader(status)
}

func WriteErrorResponseNoHeader(response *restful.Response, request *restful.Request, err error, resource string) {
	// HEAD should have no body, do not attempt to write to it
	if request.Request.Method == "HEAD" {
		return
	}

	// Generate error response.
	errorResponse := ApiErrorResponse{}
	apiErrorCode, ok := err.(ApiError)
	if ok {
		errorResponse.AwsErrorCode = apiErrorCode.AwsErrorCode()
		errorResponse.Message = apiErrorCode.Description()
	} else {
		errorResponse.AwsErrorCode = "InternalError"
		errorResponse.Message = "We encountered an internal error, please try again."
	}
	errorResponse.Resource = resource
	//errorResponse.RequestId = requestIdFromContext(req.Context())
	errorResponse.HostId = helper.CONFIG.InstanceId

	//encodedErrorResponse := EncodeResponse(errorResponse)

	//ResponseRecorder
	//w.(*ResponseRecorder).size = int64(len(encodedErrorResponse))

	//w.Write(encodedErrorResponse)
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
