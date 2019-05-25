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

package utils

const (
	ActionExpiration               = 1
	ActionIncloudTransition        = 2
	ActionCrosscloudTransition     = 3
	AbortIncompleteMultipartUpload = 4
)

const (
	ActionNameExpiration = "expiration"
)

const (
	KObjKey       = "objkey"
	KLastModified = "lastmodified"
	KStorageTier  = "tier"
)

const (
	RuleStatusEnabled  = "enabled"
	RuleStatusDisabled = "disabled"
)

type Database struct {
	Credential string `conf:"credential,username:password@tcp(ip:port)/dbname"`
	Driver     string `conf:"driver,mongodb"`
	Endpoint   string `conf:"endpoint,localhost:27017"`
}

type InternalLifecycleFilter struct {
	Prefix string
	Tags   []string
}

type InternalLifecycleRule struct {
	Id           string
	Bucket       string
	Filter       InternalLifecycleFilter
	Days         int32
	Tier         int32
	ActionType   int // 0-Expiration, 1-IncloudTransition, 2-CrossCloudTransition, 3-AbortMultipartUpload
	DeleteMarker string
	Backend      string
}
