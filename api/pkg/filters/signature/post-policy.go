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
package signature

import (
	"regexp"
)

type PostPolicyType int

const (
	PostPolicyUnknown PostPolicyType = iota
	PostPolicyV2
	PostPolicyV4
	PostPolicyAnonymous
)

var (
	// Convert to Canonical Form before compare
	EqPolicyRegExpV2 = regexp.MustCompile("(?i)Acl|Bucket|Cache-Control|Content-Type|Content-Disposition" +
		"|Content-Encoding|Expires|Key|Success_action_redirect|Redirect|Success_action_status" +
		"|X-Amz-Meta-.+")
	StartsWithPolicyRegExpV2 = regexp.MustCompile("(?i)Acl|Cache-Control|Content-Type|Content-Disposition" +
		"|Content-Encoding|Expires|Key|Success_action_redirect|Redirect|X-Amz-Meta-.+")
	IgnoredFormRegExpV2 = regexp.MustCompile("(?i)Awsaccesskeyid|Signature|File|Policy|X-Ignore-.+")
)

func GetPostPolicyType(formValues map[string]string) PostPolicyType {
	if _, ok := formValues["Policy"]; !ok {
		return PostPolicyAnonymous
	}
	if _, ok := formValues["Signature"]; ok {
		return PostPolicyV2
	}
	if algorithm, ok := formValues["X-Amz-Algorithm"]; ok {
		if algorithm == SignV4Algorithm {
			return PostPolicyV4
		}
	}
	return PostPolicyUnknown
}
