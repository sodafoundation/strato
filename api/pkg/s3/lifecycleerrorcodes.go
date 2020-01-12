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

const (
	DuplicateRuleIDError               = "error: PUT bucket lifecycle failed because the ruleId %s is duplicate."
	NoRequestBodyLifecycle             = "error: No request body specified for creating lifecycle configuration"
	NoRequestBodySSE                   = "error: No request body specified for creating SSE configuration"
	MoreThanOneExpirationAction        = "error: More than one expiration action is not permitted in one rule"
	DaysInStorageClassBeforeExpiration = "error: minimum days for an object in the current storage class should be less than Expiration Days"
	DaysInStorageClassBeforeTransition = "error: minimum days for an object in the current storage class is less before transition action"
	NoRuleIDForLifecycleDelete         = "error: No rule ID specified to delete from lifecycle configuration"
	WrongRuleIDForLifecycleDelete      = "error: The rule ID which is specified for delete does not exist"
	InvalidTier                        = "error: Invalid tier"
	InvalidExpireDays                  = "error: Days for Expiring object must not be less than %d"
	InvalidTransistionDays             = "error: days for transitioning object to tier_%d must not be less than %d"
	TooMuchLCRuls                      = "error: number of rules should not more than %d"
)

const (
	AWSErrCodeInvalidArgument = "InvalidArgument"
)
