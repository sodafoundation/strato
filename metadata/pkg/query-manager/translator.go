// Copyright 2023 The SODA Authors.
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

package query_manager

import (
	"github.com/opensds/multi-cloud/metadata/pkg/constants"
	pb "github.com/opensds/multi-cloud/metadata/proto"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
)

func Translate(in *pb.ListMetadataRequest) []bson.D {
	var aggOperations []bson.D

	//* Below is a sample aggregation , we are using to query the database
	//db.metadatabucket.aggregate( [
	//
	//   {
	//      $match: { backendName: "bakend1" }
	//   }
	// ] )
	// Info on implementing aggregation in golang can be found in
	//* https://www.mongodb.com/docs/drivers/go/current/fundamentals/aggregation/

	aggOperations = constructAggOperationForBackendLevel(in, aggOperations)
	aggOperations = constructAggOperationForObjectLevel(in, aggOperations)
	aggOperations = constructAggOperationForBucketLevel(in, aggOperations)
	aggOperations = sortAggOperation(aggOperations, constants.BACKEND_NAME, in.SortOrder)
	return aggOperations
}

func sortAggOperation(aggOperations []bson.D, fieldName string, order string) []bson.D {
	sortOrder := constants.ASCENDING_ORDER

	if order == constants.DESC {
		sortOrder = constants.DESCENDING_ORDER
	}

	sortAgg := bson.D{{constants.SORT_AGG_OP, bson.D{{fieldName, sortOrder}}}}
	aggOperations = append(aggOperations, sortAgg)
	return aggOperations
}

func constructAggOperationForObjectLevel(in *pb.ListMetadataRequest, aggOperations []bson.D) []bson.D {
	filterConditions := bson.A{}
	var matchConditions []bson.E

	if in.ObjectName != "" {
		objectNameFilterCondition := GetConditionForOperator(constants.OBJECT_NAME, in.ObjectName, constants.EQUAL_OPERATOR)
		filterConditions = append(filterConditions, objectNameFilterCondition)
		matchConditions = append(matchConditions, GetMatchingConditions(constants.OBJECTS_NAME, in.ObjectName, constants.EQUAL_OPERATOR))
	}

	if in.ObjectSizeOperator != "" {
		objectSizeFilterCondition := GetConditionForOperator(constants.OBJECT_SIZE, in.SizeOfObjectInBytes, in.ObjectSizeOperator)
		filterConditions = append(filterConditions, objectSizeFilterCondition)
		matchConditions = append(matchConditions, GetMatchingConditions(constants.OBJECTS_SIZE, in.SizeOfObjectInBytes, in.ObjectSizeOperator))
	}

	if len(filterConditions) > 0 {
		findMatchingDocuments := bson.D{{constants.MATCH_AGG_OP, bson.D{{
			constants.BUCKETS, bson.D{{
				constants.ELEMMATCH_AGG_OPERATOR,
				bson.D(matchConditions),
			}},
		}}}}

		log.Debugln("matching documents query for object level:", findMatchingDocuments)

		filterOnlyRequiredObjects := bson.D{{constants.PROJECT_AGG_OPERATOR, bson.D{
			{constants.ID, constants.INCLUDE_FIELD},
			{constants.BACKEND_NAME, constants.INCLUDE_FIELD},
			{constants.REGION, constants.INCLUDE_FIELD},
			{constants.TYPE, constants.INCLUDE_FIELD},
			{constants.NUMBER_OF_BUCKETS, constants.INCLUDE_FIELD},
			getBucketsHavingFilteredObjectsQuery(filterConditions),
		}}}
		objectLevelAggOperations := []bson.D{
			findMatchingDocuments,
			filterOnlyRequiredObjects,
		}
		aggOperations = append(aggOperations, objectLevelAggOperations...)
	}

	return aggOperations
}

func getBucketsHavingFilteredObjectsQuery(filterConditions bson.A) bson.E {
	return bson.E{constants.BUCKETS, bson.D{
		{constants.MAP_AGG_OPERATOR, bson.D{
			{constants.INPUT, constants.DOLLAR_SYMBOL + constants.BUCKETS},
			{constants.AS, constants.BUCKET},
			{constants.IN, bson.D{
				//* asking to include all the rest of bucket fields in the map output
				{constants.CREATION_DATE, getQualifiedNameForMapBucket(constants.CREATION_DATE)},
				{constants.NAME, getQualifiedNameForMapBucket(constants.NAME)},
				{constants.REGION, getQualifiedNameForMapBucket(constants.REGION)},
				{constants.TYPE, getQualifiedNameForMapBucket(constants.TYPE)},
				{constants.ACCESS, getQualifiedNameForMapBucket(constants.ACCESS)},
				{constants.NUMBER_OF_OBJECTS, getQualifiedNameForMapBucket(constants.NUMBER_OF_OBJECTS)},
				{constants.NUMBER_OF_FILTERED_OBJECTS, getTotalNumberOfObjects(filterConditions)},
				{constants.TOTAL_SIZE, getQualifiedNameForMapBucket(constants.TOTAL_SIZE)},
				{constants.FILTERED_BUCKET_SIZE, getTotalSizeForObjects(filterConditions)},
				{constants.TAGS, getQualifiedNameForMapBucket(constants.TAGS)},
				//* asking to filter the objects array based on the object level queries given by user
				{constants.OBJECTS, getFilteredObjectsQuery(filterConditions)},
			},
			},
		},
		},
	},
	}
}

func getTotalSizeForObjects(filterConditions bson.A) bson.D {
	return bson.D{{"$sum", bson.D{
		{constants.MAP_AGG_OPERATOR, bson.D{
			{constants.INPUT, getFilteredObjectsQuery(filterConditions)},
			{constants.AS, constants.OBJECT},
			{constants.IN, "$$object.size"}},
		}}}}
}

func getTotalNumberOfObjects(filterConditions bson.A) bson.D {
	return bson.D{{"$size", getFilteredObjectsQuery(filterConditions)}}
}

func getTotalNumberOfBuckets(filterConditions bson.A) bson.D {
	return bson.D{{"$size", getFilteredBucketsQuery(filterConditions)}}
}

func getFilteredObjectsQuery(filterConditions bson.A) bson.D {
	return bson.D{

		{constants.FILTER_AGG_OP, bson.D{
			{constants.INPUT, getQualifiedNameForMapBucket(constants.OBJECTS)},
			{constants.AS, constants.OBJECT},
			{constants.COND_OPERATOR, bson.D{{constants.AND_OPERATOR, filterConditions}}},
		},
		},
	}
}

func getQualifiedNameForMapBucket(fieldName string) string {
	return constants.DOLLAR_DOLLAR + constants.BUCKET + constants.DOT + fieldName
}

func constructAggOperationForBackendLevel(in *pb.ListMetadataRequest, aggOperations []bson.D) []bson.D {
	aggOperations = GetMatchAggOperation(constants.BACKEND_NAME, in.BackendName, aggOperations)
	aggOperations = GetMatchAggOperation(constants.REGION, in.Region, aggOperations)
	aggOperations = GetMatchAggOperation(constants.TYPE, in.Type, aggOperations)

	return aggOperations
}

func constructAggOperationForBucketLevel(in *pb.ListMetadataRequest, aggOperations []bson.D) []bson.D {
	filterConditions := bson.A{}
	var matchConditions []bson.E

	if in.BucketName != "" {
		bucketNameMatchingCondition := GetMatchingConditions(constants.NAME, in.BucketName, constants.EQUAL_OPERATOR)
		bucketNameFilterCondition := GetConditionForOperator(constants.BUCKET_NAME, in.BucketName, constants.EQUAL_OPERATOR)
		filterConditions = append(filterConditions, bucketNameFilterCondition)
		matchConditions = append(matchConditions, bucketNameMatchingCondition)
	}

	if isObjectLevelQueryPresent(in) {
		// if object level query is present then display only the buckets having non-empty objects after filtering
		// i.e. numberOfFilteredObjects field exists (since it is added only if object level query is present)
		bucketsHavingFilteredObjectsFilter := bson.D{{"$ifNull", bson.A{"$$bucket.numberOfFilteredObjects", false}}}
		filterConditions = append(filterConditions, bucketsHavingFilteredObjectsFilter)
	}

	if in.BucketSizeOperator != "" {
		bucketSizeMatchingCondition := GetMatchingConditions(constants.TOTAL_SIZE, in.SizeOfBucketInBytes, in.BucketSizeOperator)
		bucketSizeFilterCondition := GetConditionForOperator(constants.BUCKET_TOTAL_SIZE, in.SizeOfBucketInBytes, in.BucketSizeOperator)
		filterConditions = append(filterConditions, bucketSizeFilterCondition)
		matchConditions = append(matchConditions, bucketSizeMatchingCondition)
	}

	var bucketLevelAggOperations []bson.D
	if len(matchConditions) > 0 {
		findMatchingDocuments := bson.D{{constants.MATCH_AGG_OP, bson.D{{
			constants.BUCKETS, bson.D{{
				constants.ELEMMATCH_AGG_OPERATOR,
				bson.D(matchConditions),
			}},
		}}}}
		bucketLevelAggOperations = append(bucketLevelAggOperations, findMatchingDocuments)
	}
	if len(filterConditions) > 0 {
		filterOnlyRequiredObjects := bson.D{
			{constants.PROJECT_AGG_OP, bson.D{
				{constants.ID, constants.INCLUDE_FIELD},
				{constants.BACKEND_NAME, constants.INCLUDE_FIELD},
				{constants.REGION, constants.INCLUDE_FIELD},
				{constants.TYPE, constants.INCLUDE_FIELD},
				{constants.BUCKETS, getFilteredBucketsQuery(filterConditions)},
				{constants.NUMBER_OF_BUCKETS, constants.INCLUDE_FIELD},
				{constants.NUMBER_OF_FILTERED_BUCKETS, getTotalNumberOfBuckets(filterConditions)},
			}},
		}
		bucketLevelAggOperations = append(bucketLevelAggOperations, filterOnlyRequiredObjects)
	}

	if len(bucketLevelAggOperations) > 0 {
		aggOperations = append(aggOperations, bucketLevelAggOperations...)
	}

	return aggOperations
}

func isObjectLevelQueryPresent(in *pb.ListMetadataRequest) bool {
	return in.ObjectName != "" || in.ObjectSizeOperator != ""
}

func getFilteredBucketsQuery(filterConditions bson.A) bson.D {
	return bson.D{
		{constants.FILTER_AGG_OP, bson.D{
			{constants.INPUT, constants.DOLLAR_SYMBOL + constants.BUCKETS},
			{constants.AS, constants.BUCKET},
			{constants.COND_OPERATOR, bson.D{
				{constants.AND_OPERATOR, filterConditions},
			}},
		}},
	}
}

func GetConditionForOperator(fieldName string, field interface{}, operator string) bson.D {
	return bson.D{{constants.DOLLAR_SYMBOL + operator, bson.A{constants.DOLLAR_SYMBOL + constants.DOLLAR_SYMBOL + fieldName, field}}}
}

func GetMatchingConditions(fieldName string, fieldValue interface{}, sizeOperator string) bson.E {
	return bson.E{Key: fieldName, Value: bson.D{{Key: constants.DOLLAR_SYMBOL + sizeOperator, Value: fieldValue}}}
}

func GetMatchAggOperation(fieldName string, paramValue string, aggOperations []bson.D) []bson.D {
	if paramValue != "" {
		matchRegionStage := bson.D{{Key: constants.MATCH_AGG_OP, Value: bson.D{{Key: fieldName, Value: paramValue}}}}
		return append(aggOperations, matchRegionStage)
	}
	return aggOperations
}
