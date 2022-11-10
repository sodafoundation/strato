// Copyright 2019 The soda Authors.
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

package _exception

import (
	"errors"
	"fmt"
)

type S3Error struct {
	Code        int
	Description string
}

func (err *S3Error) Error() error {
	s := fmt.Sprintf("{\"code\":\"%d\",\"message\":\"%s\"}", err.Code, err.Description)
	return errors.New(s)
}

var ERR_OK = 200
var NoError = S3Error{Code: ERR_OK}
var InternalError = S3Error{Code: 500, Description: "Internal error. Please retry"}
var NoSuchBucket = S3Error{Code: 404, Description: "The specified bucket does not exist."}
var DBError = S3Error{Code: 500, Description: "DB occured exception."}
var NoSuchObject = S3Error{Code: 404, Description: "The specified object does not exist."}
var BucketAlreadyExists = S3Error{Code: 409, Description: "The requested bucket name already exist. Bucket namespace is shared by all users in the system. Select a different name and retry."}

var NoSuchBackend = S3Error{Code: 404, Description: "The specified backend does not exists."}
var NoSuchType = S3Error{Code: 404, Description: "The specified backend type does not exists."}
var BucketDeleteError = S3Error{Code: 500, Description: "The bucket can not be deleted. please delete object first"}
var BackendDeleteError = S3Error{Code: 500, Description: "The backend can not be deleted. please delete bucket first"}

var InvalidQueryParameter = S3Error{Code: 400, Description: "invalid query parameter"}
var InvalidContentLength = S3Error{Code: 400, Description: "invalid content length"}
var InvalidStorageClass = S3Error{Code: 400, Description: "the storage class you specified is not valid"}
var BadRequest = S3Error{Code: 400, Description: "request is invalid"}
