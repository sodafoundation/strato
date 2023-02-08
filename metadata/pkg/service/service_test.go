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

package service

import (
	"context"
	"github.com/opensds/multi-cloud/metadata/pkg/db"
	"github.com/opensds/multi-cloud/metadata/pkg/model"
	query_manager "github.com/opensds/multi-cloud/metadata/pkg/query-manager"
	pb "github.com/opensds/multi-cloud/metadata/proto"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"testing"
)

var listService = NewMetaService()

type listMetaDataTestcase struct {
	title          string
	input          pb.ListMetadataRequest
	expectedOutput pb.ListMetadataResponse
	expectedErr    error
}

type mockDbAdapter struct{}

func (mockDbAdapter) CreateMetadata(ctx context.Context, metaBackend model.MetaBackend) error {
	return nil
}

func (mockDbAdapter) ListMetadata(ctx context.Context, query []bson.D) ([]*model.MetaBackend, error) {
	return nil, nil
}

func Test_ListMetadata(t *testing.T) {
	db.DbAdapter = &mockDbAdapter{}

	testcases := []listMetaDataTestcase{
		//* Empty list metadata request
		{
			title:          "Empty request",
			input:          pb.ListMetadataRequest{},
			expectedOutput: pb.ListMetadataResponse{},
			expectedErr:    nil,
		},
		//* Invalid list metadata request
		{
			title:          "Invalid request",
			input:          pb.ListMetadataRequest{BucketSizeOperator: "invalid"},
			expectedOutput: pb.ListMetadataResponse{},
			expectedErr:    &query_manager.ValidationError{ErrMsg: "Operator for size should be  lte, gt, eq, gte or lt"},
		},
	}

	for _, test := range testcases {
		output := pb.ListMetadataResponse{}
		err := listService.ListMetadata(context.TODO(), &test.input, &output)
		if err != nil {
			assert.Equalf(t, err, test.expectedErr, test.title)
		} else {
			assert.Equalf(t, output, test.expectedOutput, test.title)
		}
	}
}
