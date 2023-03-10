/*
 * // Copyright 2023 The SODA Authors.
 * //
 * // Licensed under the Apache License, Version 2.0 (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * //     http://www.apache.org/licenses/LICENSE-2.0
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 *
 */

package utils

import (
	"github.com/opensds/multi-cloud/metadata/pkg/model"
	"testing"
	"time"
)

func TestGetBackends(t *testing.T) {
	now := time.Now()
	unPaginatedResult := []*model.MetaBackend{{
		BackendName:     "backend1",
		Region:          "region1",
		Type:            "type1",
		NumberOfBuckets: 1,
		Buckets: []*model.MetaBucket{{
			Name:            "bucket1",
			Type:            "type1",
			Region:          "region1",
			TotalSize:       100,
			NumberOfObjects: 1,
			CreationDate:    &now,
			BucketTags:      map[string]string{"key1": "value1"},
			Objects: []*model.MetaObject{{
				ObjectName:           "object1",
				LastModifiedDate:     &now,
				ServerSideEncryption: "true",
				ExpiresDate:          &now,
				GrantControl:         "grantControl1",
				RedirectLocation:     "redirect1",
				ReplicationStatus:    "replicated",
				ObjectTags:           map[string]string{"key1": "value1"},
				Metadata:             map[string]string{"key1": "value1"},
			}},
		}},
	}}

	protoBackends := GetBackends(unPaginatedResult)

	if len(protoBackends) != 1 {
		t.Errorf("Expected 1 proto backend, but got %d", len(protoBackends))
	}

	if protoBackends[0].BackendName != "backend1" {
		t.Errorf("Expected backend name to be backend1, but got %s", protoBackends[0].BackendName)
	}

	if len(protoBackends[0].Buckets) != 1 {
		t.Errorf("Expected 1 proto bucket, but got %d", len(protoBackends[0].Buckets))
	}

	if len(protoBackends[0].Buckets[0].Objects) != 1 {
		t.Errorf("Expected 1 proto object, but got %d", len(protoBackends[0].Buckets[0].Objects))
	}
}
