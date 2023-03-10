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
	pb "github.com/opensds/multi-cloud/metadata/proto"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"testing"
)

func Test_Translator(t *testing.T) {
	sortAscBasedOnBackName := bson.D{{"$sort", bson.D{{"backendName", 1}}}}
	
	backendNameMatch := bson.D{
		bson.E{Key: "$match", Value: bson.D{
			bson.E{Key: "backendName", Value: "backendName"}}}}

	bucketNameMatch := bson.D{
		bson.E{Key: "$match", Value: bson.D{
			bson.E{Key: "buckets", Value: bson.D{
				bson.E{Key: "$elemMatch", Value: bson.D{
					bson.E{Key: "name", Value: bson.D{
						bson.E{Key: "$eq", Value: "bucketName"}}}}}}}}}}

	bucketNameFilter := bson.D{
		bson.E{Key: "$project", Value: bson.D{
			bson.E{Key: "_id", Value: 1},
			bson.E{Key: "backendName", Value: 1},
			bson.E{Key: "region", Value: 1},
			bson.E{Key: "type", Value: 1},
			bson.E{Key: "buckets", Value: bson.D{
				bson.E{Key: "$filter", Value: bson.D{
					bson.E{Key: "input", Value: "$buckets"},
					bson.E{Key: "as", Value: "bucket"},
					bson.E{Key: "cond", Value: bson.D{
						bson.E{Key: "$and", Value: bson.A{bson.D{
							bson.E{Key: "$eq", Value: bson.A{"$$bucket.name", "bucketName"}}}}}}}}}}},
			bson.E{Key: "numberOfBuckets", Value: 1},
			bson.E{Key: "numberOFilteredBuckets", Value: bson.D{
				bson.E{Key: "$size", Value: bson.D{
					bson.E{Key: "$filter", Value: bson.D{
						bson.E{Key: "input", Value: "$buckets"},
						bson.E{Key: "as", Value: "bucket"},
						bson.E{Key: "cond",
							Value: bson.D{
								bson.E{Key: "$and",
									Value: bson.A{bson.D{
										bson.E{Key: "$eq", Value: bson.A{"$$bucket.name", "bucketName"}}}}}}}}}}}}}}}}

	objectNameMatch := bson.D{
		bson.E{Key: "$match", Value: bson.D{
			bson.E{Key: "buckets", Value: bson.D{
				bson.E{Key: "$elemMatch", Value: bson.D{
					bson.E{Key: "objects.name", Value: bson.D{
						bson.E{Key: "$eq", Value: "objectName"}}}}}}}}}}

	objectNameFilter := bson.D{
		bson.E{Key: "$project", Value: bson.D{
			bson.E{Key: "_id", Value: 1},
			bson.E{Key: "backendName", Value: 1},
			bson.E{Key: "region", Value: 1},
			bson.E{Key: "type", Value: 1},
			bson.E{Key: "numberOfBuckets", Value: 1},
			bson.E{Key: "buckets",
				Value: bson.D{
					bson.E{Key: "$map", Value: bson.D{
						bson.E{Key: "input", Value: "$buckets"},
						bson.E{Key: "as", Value: "bucket"},
						bson.E{Key: "in", Value: bson.D{
							bson.E{Key: "creationDate", Value: "$$bucket.creationDate"},
							bson.E{Key: "name", Value: "$$bucket.name"},
							bson.E{Key: "region", Value: "$$bucket.region"},
							bson.E{Key: "type", Value: "$$bucket.type"},
							bson.E{Key: "access", Value: "$$bucket.access"},
							bson.E{Key: "numberOfObjects", Value: "$$bucket.numberOfObjects"},
							bson.E{Key: "numberOfFilteredObjects", Value: bson.D{
								bson.E{Key: "$size", Value: bson.D{
									bson.E{Key: "$filter", Value: bson.D{
										bson.E{Key: "input", Value: "$$bucket.objects"},
										bson.E{Key: "as", Value: "object"},
										bson.E{Key: "cond", Value: bson.D{
											bson.E{Key: "$and", Value: bson.A{bson.D{
												bson.E{Key: "$eq", Value: bson.A{"$$object.name", "objectName"}}}}}}}}}}}}},
							bson.E{Key: "totalSize", Value: "$$bucket.totalSize"},
							bson.E{Key: "filteredBucketSize", Value: bson.D{
								bson.E{Key: "$sum", Value: bson.D{
									bson.E{Key: "$map", Value: bson.D{
										bson.E{Key: "input", Value: bson.D{
											bson.E{Key: "$filter", Value: bson.D{
												bson.E{Key: "input", Value: "$$bucket.objects"},
												bson.E{Key: "as", Value: "object"},
												bson.E{Key: "cond", Value: bson.D{
													bson.E{Key: "$and", Value: bson.A{bson.D{
														bson.E{Key: "$eq", Value: bson.A{"$$object.name", "objectName"}}}}}}}}}}},
										bson.E{Key: "as", Value: "object"},
										bson.E{Key: "in", Value: "$$object.size"}}}}}}},
							bson.E{Key: "tags", Value: "$$bucket.tags"},
							bson.E{Key: "objects", Value: bson.D{
								bson.E{Key: "$filter", Value: bson.D{
									bson.E{Key: "input", Value: "$$bucket.objects"},
									bson.E{Key: "as", Value: "object"},
									bson.E{Key: "cond", Value: bson.D{
										bson.E{Key: "$and", Value: bson.A{bson.D{
											bson.E{Key: "$eq", Value: bson.A{"$$object.name", "objectName"}}}}}}}}}}}}}}}}}}}}
	removeEmptyBucketsFilter := bson.D{
		bson.E{Key: "$project", Value: bson.D{
			bson.E{Key: "_id", Value: 1},
			bson.E{Key: "backendName", Value: 1},
			bson.E{Key: "region", Value: 1},
			bson.E{Key: "type", Value: 1},
			bson.E{Key: "buckets", Value: bson.D{
				bson.E{Key: "$filter", Value: bson.D{
					bson.E{Key: "input", Value: "$buckets"},
					bson.E{Key: "as", Value: "bucket"},
					bson.E{Key: "cond", Value: bson.D{
						bson.E{Key: "$and", Value: bson.A{bson.D{
							bson.E{Key: "$ifNull", Value: bson.A{"$$bucket.numberOfFilteredObjects", false}}}}}}}}}}},
			bson.E{Key: "numberOfBuckets", Value: 1},
			bson.E{Key: "numberOFilteredBuckets", Value: bson.D{
				bson.E{Key: "$size", Value: bson.D{
					bson.E{Key: "$filter", Value: bson.D{
						bson.E{Key: "input", Value: "$buckets"},
						bson.E{Key: "as", Value: "bucket"},
						bson.E{Key: "cond", Value: bson.D{
							bson.E{Key: "$and", Value: bson.A{bson.D{
								bson.E{Key: "$ifNull", Value: bson.A{"$$bucket.numberOfFilteredObjects", false}}}}}}}}}}}}}}}}

	tests := []struct {
		title          string
		input          pb.ListMetadataRequest
		expectedOutput interface{}
	}{
		{
			title:          "Empty Request",
			input:          pb.ListMetadataRequest{},
			expectedOutput: []bson.D{sortAscBasedOnBackName},
		},
		{
			title:          "Query Backend Name",
			input:          pb.ListMetadataRequest{BackendName: "backendName"},
			expectedOutput: []bson.D{backendNameMatch, sortAscBasedOnBackName},
		},
		{
			title:          "Query Bucket Name",
			input:          pb.ListMetadataRequest{BucketName: "bucketName"},
			expectedOutput: []bson.D{bucketNameMatch, bucketNameFilter, sortAscBasedOnBackName},
		},
		{
			title:          "Query Object Name",
			input:          pb.ListMetadataRequest{ObjectName: "objectName"},
			expectedOutput: []bson.D{objectNameMatch, objectNameFilter, removeEmptyBucketsFilter, sortAscBasedOnBackName},
		},
	}

	for _, test := range tests {
		output := Translate(&test.input)
		assert.Equal(t, test.expectedOutput, output, test.title)
	}
}
