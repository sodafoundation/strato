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
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/opensds/multi-cloud/metadata/pkg/constants"
	pb "github.com/opensds/multi-cloud/metadata/proto"
)

func TestValidateInput(t *testing.T) {
	tests := []struct {
		input *pb.ListMetadataRequest
		want  bool
		err   string
	}{
		{
			input: &pb.ListMetadataRequest{
				SizeOfBucketInBytes: 0,
				BucketSizeOperator:  "",
				SizeOfObjectInBytes: 0,
				ObjectSizeOperator:  "",
				SortOrder:           constants.ASC,
				Region:              "us-west-2",
				Type:                constants.AWS_S3,
			},
			want: true,
			err:  "",
		},
		{
			input: &pb.ListMetadataRequest{
				SizeOfBucketInBytes: 0,
				BucketSizeOperator:  "",
				SizeOfObjectInBytes: 0,
				ObjectSizeOperator:  "",
				SortOrder:           constants.ASC,
				Region:              "invalid-region",
				Type:                constants.AWS_S3,
			},
			want: false,
			err:  "Not a valid AWS_S3 region",
		},
		{
			input: &pb.ListMetadataRequest{
				SizeOfBucketInBytes: 0,
				BucketSizeOperator:  "",
				SizeOfObjectInBytes: 0,
				ObjectSizeOperator:  "",
				SortOrder:           constants.ASC,
				Region:              "us-west-2",
				Type:                "invalid-type",
			},
			want: false,
			err:  "Not a valid cloud type",
		},
		{
			input: &pb.ListMetadataRequest{
				SizeOfBucketInBytes: 0,
				BucketSizeOperator:  "",
				SizeOfObjectInBytes: 0,
				ObjectSizeOperator:  "",
				SortOrder:           "invalid-sort",
				Region:              "us-west-2",
				Type:                constants.AWS_S3,
			},
			want: false,
			err:  "Invalid sort order",
		},
	}

	for _, tc := range tests {
		result, _ := ValidateInput(tc.input)
		assert.Equal(t, tc.want, result)
	}
}

func TestIsSortParamValid(t *testing.T) {
	tests := []struct {
		sortOrder string
		expected  bool
	}{
		{"", true},
		{"ASC", true},
		{"DESC", true},
		{"abc", false},
	}

	for _, test := range tests {
		result, _ := isSortParamValid(test.sortOrder)
		if result != test.expected {
			t.Errorf("isSortParamValid(%q) = %v, want %v", test.sortOrder, result, test.expected)
		}
	}
}

func TestIsSizeParamsValid(t *testing.T) {
	tests := []struct {
		sizeInBytes int64
		operator    string
		expected    bool
	}{
		{0, "", true},
		{0, "gt", true},
		{0, "lte", true},
		{0, "eq", true},
		{0, "gte", true},
		{0, "lt", true},
		{1, "gt", true},
		{1, "lte", true},
		{1, "eq", true},
		{1, "gte", true},
		{1, "lt", true},
		{-1, "gt", false},
		{-1, "lte", false},
		{-1, "eq", false},
		{-1, "gte", false},
		{-1, "lt", false},
		{1, "abc", false},
	}

	for _, test := range tests {
		result, _ := isSizeParamsValid(test.sizeInBytes, test.operator)
		if result != test.expected {
			t.Errorf("isSizeParamsValid(%d, %q) = %v, want %v", test.sizeInBytes, test.operator, result, test.expected)
		}
	}
}

func TestIsValidRegion(t *testing.T) {
	tests := []struct {
		cloudType string
		region    string
		expected  bool
	}{
		{"AWS-S3", "us-west-1", true},
		{"AWS-S3", "us-west-2", true},
		{"AWS-S3", "abc", false},
		{"", "", true},
		{"abc", "us-west-1", false},
	}

	for _, test := range tests {
		result, _ := isValidRegion(test.region, test.cloudType)
		if result != test.expected {
			t.Errorf("isValidRegion(%q, %q) = %v, want %v", test.region, test.cloudType, result, test.expected)
		}
	}
}
