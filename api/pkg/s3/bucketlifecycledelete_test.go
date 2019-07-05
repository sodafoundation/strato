// Copyright 2019 The OpenSDS Authors.

//

// Licensed under the Apache License, Version 2.0 (the "License");

// you may not use this file except in compliance with the License.

// You may obtain a copy of the License at

//

//     http://www.apache.org/licenses/LICENSE-2.0

//

// Unless required by applicable law or agreed to in writing, software

// distributed under the License is distributed on an "AS IS" BASIS,

// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

// See the License for the specific language governing permissions and

// limitations under the License.

package s3

import (
	"testing"

	"github.com/emicklei/go-restful"
	backend "github.com/opensds/multi-cloud/backend/proto"
	s3 "github.com/opensds/multi-cloud/s3/proto"
)

func TestAPIService_BucketLifecycleDelete(t *testing.T) {
	type fields struct {
		s3Client      s3.S3Service
		backendClient backend.BackendService
	}
	type args struct {
		request  *restful.Request
		response *restful.Response
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &APIService{
				s3Client:      tt.fields.s3Client,
				backendClient: tt.fields.backendClient,
			}
			s.BucketLifecycleDelete(tt.args.request, tt.args.response)
		})
	}
}
