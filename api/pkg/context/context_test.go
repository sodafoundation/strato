// Copyright 2019 The soda Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package context

import (
	"reflect"
	"testing"
)

func TestContext(t *testing.T) {
	ctx := Context{
		AuthToken:     "token-123456789",
		UserId:        "ebf133af8beb474f962869ec0d362b1e",
		IsAdmin:       true,
		IsAdminTenant: false,
	}
	expect := map[string]interface{}{
		"auth_token":      "token-123456789",
		"user_id":         "ebf133af8beb474f962869ec0d362b1e",
		"is_admin":        true,
		"is_admin_tenant": false,
	}
	result := ctx.ToPolicyValue()
	if !reflect.DeepEqual(expect, result) {
		t.Errorf("Test Context ToPolicyValue failed, expected:%v, get:%v", expect, result)
	}
}

func TestToJson(t *testing.T) {
	ctx := Context{
		AuthToken:     "token-123456789",
		UserId:        "ebf133af8beb474f962869ec0d362b1e",
		IsAdmin:       true,
		IsAdminTenant: false,
	}

	expect := Context{
		IsAdmin:       true,
		AuthToken:     "token-123456789",
		UserId:        "ebf133af8beb474f962869ec0d362b1e",
		IsAdminTenant: false,
	}

	result := ctx.ToJson()

	if !reflect.DeepEqual(expect, result) {
		t.Errorf("Test Context ToJson filed, expected :%v, get:%v", expect, result)
	}

}
