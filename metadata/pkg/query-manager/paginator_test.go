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

package query_manager

import (
	"github.com/opensds/multi-cloud/metadata/pkg/model"
	"testing"
)

func TestPaginate(t *testing.T) {
	tests := []struct {
		unPaginatedResult []*model.MetaBackend
		limit             int32
		offset            int32
		expectedResult    []*model.MetaBackend
	}{
		{
			unPaginatedResult: []*model.MetaBackend{
				{Id: "1"},
				{Id: "2"},
				{Id: "3"},
				{Id: "4"},
				{Id: "5"},
			},
			limit:  2,
			offset: 0,
			expectedResult: []*model.MetaBackend{
				{Id: "1"},
				{Id: "2"},
			},
		},
		{
			unPaginatedResult: []*model.MetaBackend{
				{Id: "1"},
				{Id: "2"},
				{Id: "3"},
				{Id: "4"},
				{Id: "5"},
			},
			limit:  2,
			offset: 2,
			expectedResult: []*model.MetaBackend{
				{Id: "3"},
				{Id: "4"},
			},
		},
		{
			unPaginatedResult: []*model.MetaBackend{
				{Id: "1"},
				{Id: "2"},
				{Id: "3"},
				{Id: "4"},
				{Id: "5"},
			},
			limit:          2,
			offset:         5,
			expectedResult: []*model.MetaBackend{},
		},
		{
			unPaginatedResult: []*model.MetaBackend{},
			limit:             2,
			offset:            0,
			expectedResult:    []*model.MetaBackend{},
		},
	}
	for i, test := range tests {
		result := Paginate(test.unPaginatedResult, test.limit, test.offset)
		if len(result) != len(test.expectedResult) {
			t.Errorf("Test case %d: expected length of result to be %d but got %d", i, len(test.expectedResult), len(result))
		}
		for j, r := range result {
			if r.Id != test.expectedResult[j].Id {
				t.Errorf("Test case %d: expected result at index %d to be %s but got %s", i, j, test.expectedResult[j].Id, r.Id)
			}
		}
	}
}
