// Copyright (c) 2018 Huawei Technologies Co., Ltd. All Rights Reserved.
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

import "github.com/opensds/multi-cloud/s3/proto"

type LocationInfo struct {
	StorType string //aws-s3,azure-blob,hw-obs,etc.
	Region string
	EndPoint string
	BucketName string //remote bucket name
	Access string
	Security string
	BakendId string
}

type SourceOject struct {
	StorType string //aws-s3,azure-blob,hw-obs,etc.
	Obj *s3.Object
}
