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

package utils

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"time"

	"github.com/golang/protobuf/jsonpb"
	pstruct "github.com/golang/protobuf/ptypes/struct"
)

const (
	CharacterSet = "abcdefghijklmnopqrstuvwxyz" +
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
		"0123456789" +
		"~=+%^*/()[]{}/!@#$?|"
)

func RandString(length int) string {
	randVal := rand.New(rand.NewSource(time.Now().UnixNano()))
	bytes := make([]byte, length)
	for i := range bytes {
		bytes[i] = CharacterSet[randVal.Intn(len(CharacterSet))]
	}
	return string(bytes)
}

func ConvertMapToStruct(m map[string]interface{}) (*pstruct.Struct, error) {

	byteArray, err := json.Marshal(m)

	if err != nil {
		return nil, err
	}

	reader := bytes.NewReader(byteArray)

	pbs := &pstruct.Struct{}
	if err = jsonpb.Unmarshal(reader, pbs); err != nil {
		return nil, err
	}

	return pbs, nil
}
