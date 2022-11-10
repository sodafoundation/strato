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
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"strings"

	. "github.com/soda/multi-cloud/api/pkg/s3/datatype"
	. "github.com/soda/multi-cloud/s3/error"
	"github.com/soda/multi-cloud/s3/pkg/helper"
)

// validates location constraint from the request body.
// the location value in the request body should match the Region in serverConfig.
// other values of location are not accepted.
// make bucket fails in such cases.
func isValidLocationConstraint(reqBody io.Reader) (err error) {
	var region = helper.CONFIG.Region
	var locationConstraint CreateBucketLocationConfiguration
	e := xmlDecoder(reqBody, &locationConstraint)
	if e != nil {
		if e == io.EOF {
			// Failed due to empty request body. The location will be set to
			// default value from the serverConfig
			err = nil
		} else {
			// Failed due to malformed configuration.
			err = ErrMalformedXML
		}
	} else {
		// Region obtained from the body.
		// It should be equal to Region in serverConfig.
		// Else ErrInvalidRegion returned.
		// For empty value location will be to set to  default value from the serverConfig.
		if locationConstraint.Location != "" && region != locationConstraint.Location {
			err = ErrInvalidRegion
		}
	}
	return err
}

// Supported headers that needs to be extracted.
var supportedHeaders = []string{
	"cache-control",
	"content-disposition",
	"content-encoding",
	"content-language",
	"content-type",
	"expires",
	"website-redirect-location",
	// Add more supported headers here
}

// extractMetadataFromHeader extracts metadata from HTTP header.
func extractMetadataFromHeader(header http.Header) map[string]string {
	metadata := make(map[string]string)
	// Save standard supported headers.
	for _, supportedHeader := range supportedHeaders {
		if h := header.Get(http.CanonicalHeaderKey(supportedHeader)); h != "" {
			metadata[supportedHeader] = h
		}
	}
	// Go through all other headers for any additional headers that needs to be saved.
	for key := range header {
		if strings.HasPrefix(strings.ToLower(key), "x-amz-meta-") {
			metadata[key] = header.Get(key)
		}
	}
	// Return.
	return metadata
}

// Suffix matcher string matches suffix in a platform specific way.
// For example on windows since its case insensitive we are supposed
// to do case insensitive checks.
func hasSuffix(s string, suffix string) bool {
	return strings.HasSuffix(s, suffix)
}

func extractHTTPFormValues(reader *multipart.Reader) (filePartReader io.Reader,
	formValues map[string]string, err error) {

	formValues = make(map[string]string)
	for {
		var part *multipart.Part
		part, err = reader.NextPart()
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			return nil, nil, err
		}

		if part.FormName() != "file" {
			var buffer []byte
			buffer, err = ioutil.ReadAll(part)
			if err != nil {
				return nil, nil, err
			}
			formValues[http.CanonicalHeaderKey(part.FormName())] = string(buffer)
		} else {
			// "All variables within the form are expanded prior to validating
			// the POST policy"
			fileName := part.FileName()
			objectKey, ok := formValues["Key"]
			if !ok {
				return nil, nil, ErrMissingFields
			}
			if strings.Contains(objectKey, "${filename}") {
				formValues["Key"] = strings.Replace(objectKey, "${filename}", fileName, -1)
			}

			filePartReader = part
			// "The file or content must be the last field in the form.
			// Any fields below it are ignored."
			break
		}
	}

	if filePartReader == nil {
		err = ErrEmptyEntity
	}
	return
}
