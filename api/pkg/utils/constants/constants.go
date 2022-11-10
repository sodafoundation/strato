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

package constants

const (
	//Signature parameter name
	AuthorizationHeader = "Authorization"
	SignDateHeader      = "X-Auth-Date"

	// Token parameter name
	AuthTokenHeader   = "X-Auth-Token"
	DefaultPolicyPath = "/etc/multicloud/policy.json"
)

const (
	StorageClasssodaStandard = "STANDARD"
	StorageClassAWSStandard     = "STANDARD"
)

const (
	ActionNameExpiration = "expiration"
	ActionNameTransition = "transition"
)

const (
	ExpirationMinDays           = 1
	TransitionMinDays           = 30
	LifecycleTransitionDaysStep = 30 // The days an object should be save in the current tier before transition to the next tier
	TransitionToArchiveMinDays  = 1
)

const (
	ListObjectsType2Str string = "2"
	ListObjectsType2Int int32  = 2
	ListObjectsType1Int int32  = 1
)
