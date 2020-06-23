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

package azure

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Azure/azure-storage-file-go/azfile"
	"github.com/golang/protobuf/jsonpb"
	"github.com/micro/go-micro/v2/util/log"

	pstruct "github.com/golang/protobuf/ptypes/struct"
)

const (

	AZURE_FILESHARE_USAGE_BYTES = "Share-Usage-Bytes"

    AZURE_ETAG = "Etag"

    AZURE_LAST_MODIFIED = "Last-Modified"

	AZURE_URL = "URL"

    AZURE_X_MS_SHARE_QUOTA = "X-Ms-Share-Quota"

)

func ConvertHeaderToStruct(msg map[string][]string) (*pstruct.Struct, error) {

	byteArray, err := json.Marshal(msg)

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

func ConvertStructToAzureMetadata(pbs *pstruct.Struct) (azfile.Metadata, error) {
	fields := pbs.GetFields()

	valuesMap := make(map[string]string)

	for key, value := range fields {
		if v, ok := value.GetKind().(*pstruct.Value_StringValue); ok {
			valuesMap[key] = v.StringValue
		} else {
			msg := fmt.Sprintf("Failed to parse field for key = [%+v], value = [%+v]", key, value)
			err := errors.New(msg)
			log.Errorf(msg)
			return nil, err
		}
	}
	return valuesMap, nil
}

func ConvertStructToStructMap(msg map[string]string) (*pstruct.Struct, error) {

	byteArray, err := json.Marshal(msg)

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
