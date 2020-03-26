/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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
	"bytes"
	"encoding/xml"
	"net/http"
	"strconv"
	"time"

	"github.com/emicklei/go-restful"
	. "github.com/opensds/multi-cloud/api/pkg/s3/datatype"
	"github.com/opensds/multi-cloud/s3/pkg/meta/types"
	pb "github.com/opensds/multi-cloud/s3/proto"
)

// Refer: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTCommonResponseHeaders.html
var CommonS3ResponseHeaders = []string{"Content-Length", "Content-Type", "Connection", "Date", "ETag", "Server",
	"x-amz-delete-marker", "x-amz-id-2", "x-amz-request-id", "x-amz-version-id"}

// Encodes the response headers into XML format.
func EncodeResponse(response interface{}) []byte {
	var bytesBuffer bytes.Buffer
	bytesBuffer.WriteString(xml.Header)
	e := xml.NewEncoder(&bytesBuffer)
	e.Encode(response)
	return bytesBuffer.Bytes()
}

// Write object header
func SetObjectHeaders(response *restful.Response, object *pb.Object, contentRange *HttpRange) {
	// set object-related metadata headers
	w := response.ResponseWriter
	lastModified := time.Unix(object.LastModified, 0).UTC().Format(http.TimeFormat)
	response.ResponseWriter.Header().Set("Last-Modified", lastModified)

	w.Header().Set("Content-Type", object.ContentType)
	if object.Etag != "" {
		w.Header()["ETag"] = []string{"\"" + object.Etag + "\""}
	}

	for key, val := range object.CustomAttributes {
		w.Header().Set(key, val)
	}
	//default cache-control is no-store
	if cc, ok := object.CustomAttributes["cache-control"]; !ok {
		w.Header().Set("Cache-Control", "no-store")
	} else {
		w.Header().Set("Cache-Control", cc)
	}

	w.Header().Set("X-Amz-Object-Type", (&types.Object{Object: object}).ObjectTypeToString())
	w.Header().Set("X-Amz-Storage-Class", object.StorageClass)
	w.Header().Set("Content-Length", strconv.FormatInt(object.Size, 10))

	// for providing ranged content
	if contentRange != nil && contentRange.OffsetBegin > -1 {
		// Override content-length
		w.Header().Set("Content-Length", strconv.FormatInt(contentRange.GetLength(), 10))
		w.Header().Set("Content-Range", contentRange.String())
		w.WriteHeader(http.StatusPartialContent)
	}
}
