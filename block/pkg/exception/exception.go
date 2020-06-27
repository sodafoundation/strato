// Copyright 2020 The SODA Authors.
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

type BlockError struct {
	Code        int
	Description string
}

func (err *BlockError) Error() error {
	s := fmt.Sprintf("{\"code\":\"%d\",\"message\":\"%s\"}", err.Code, err.Description)
	return errors.New(s)
}

var ERR_OK = 200
var NoError = BlockError{Code: ERR_OK}
var InternalError = BlockError{Code: 500, Description: "Internal error. Please retry"}
var DBError = BlockError{Code: 500, Description: "DB got an exception."}

var NoSuchBackend = BlockError{Code: 404, Description: "The specified backend does not exists."}
var NoSuchType = BlockError{Code: 404, Description: "The specified backend type does not exists."}

var BadRequest = BlockError{Code: 400, Description: "request is invalid"}
