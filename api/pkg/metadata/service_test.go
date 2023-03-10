/*
 * // Copyright 2023 The SODA Authors.
 * //
 * // Licensed under the Apache License, Version 2.0 (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * //     http://www.apache.org/licenses/LICENSE-2.0
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 *
 */

package metadata

import (
	"github.com/emicklei/go-restful"
	mt "github.com/opensds/multi-cloud/metadata/proto"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/url"
	"strconv"
	"testing"
)

func GetRequest(uri string, param url.Values, t *testing.T) *restful.Request {
	httpReq, err := http.NewRequest("GET", uri+param.Encode(), nil)
	if err != nil {
		t.Fail()
	}
	req := restful.NewRequest(httpReq)
	return req
}

func Test_ListMetaData_Api_Param_Fetching(t *testing.T) {

	validParams := make(url.Values)
	validParams["limit"] = []string{"100"}
	validParams["offset"] = []string{"1"}

	invalidParams := make(url.Values)
	invalidParams["limit"] = []string{"invalid"}
	invalidParams["offset"] = []string{"1"}

	tests := []struct {
		title          string
		input          interface{}
		expectedOutput interface{}
		expectedErr    error
	}{
		{
			title:          "UT to test whether the request params are read correctly for valid values",
			input:          validParams,
			expectedOutput: mt.ListMetadataRequest{Limit: 100, Offset: 1},
			expectedErr:    nil,
		},
		{
			title:          "UT to test whether the request params is giving error  for invalid values",
			input:          invalidParams,
			expectedOutput: nil,
			expectedErr:    &strconv.NumError{},
		},
	}

	for _, test := range tests {
		uri := "http://localhost:41651/v1/metadata/metadata/?"
		values := test.input.(url.Values)
		req := GetRequest(uri, values, t)
		res, err := GetListMetaDataRequest(req)
		if err != nil {
			assert.NotNil(t, test.expectedErr, test.title)
		} else {
			assert.Equal(t, test.expectedOutput, res, test.title)
		}
	}
}
