package querytranslator

import (
	"github.com/opensds/multi-cloud/metadata/pkg/constants"
	pb "github.com/opensds/multi-cloud/metadata/proto"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
)

func Translate(in *pb.ListMetadataRequest) []bson.D {
	var aggOperations []bson.D
	aggOperations = constructAggOperationForBackendLevel(in, aggOperations)
	aggOperations = constructAggOperationForBucketLevel(in, aggOperations)
	aggOperations = constructAggOperationForObjectLevel(in, aggOperations)
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

	if in.SizeOfObjectInBytes != 0 && in.ObjectSizeOperator != "" {
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

		filterOnlyRequiredObjects := bson.D{{constants.PROJECT_AGG_OPERATOR, bson.D{{constants.ID, constants.INCLUDE_FIELD},
			{constants.BACKEND_NAME, constants.INCLUDE_FIELD},
			{constants.REGION, constants.INCLUDE_FIELD},
			{constants.TYPE, constants.INCLUDE_FIELD},
			{constants.BUCKETS, bson.D{
				{constants.MAP_AGG_OPERATOR, bson.D{
					{constants.INPUT, constants.DOLLAR_SYMBOL + constants.BUCKETS},
					{constants.AS, constants.BUCKET},
					{constants.IN, bson.D{
						//* asking to include all the rest of bucket fields in the map output
						{constants.CREATION_DATE, constants.DOLLAR_DOLLAR + constants.BUCKET + constants.DOT + constants.CREATION_DATE},
						{constants.NAME, constants.DOLLAR_DOLLAR + constants.BUCKET + constants.DOT + constants.REGION},
						{constants.REGION, constants.DOLLAR_DOLLAR + constants.BUCKET + constants.DOT + constants.REGION},
						{constants.TYPE, constants.DOLLAR_DOLLAR + constants.BUCKET + constants.DOT + constants.TYPE},
						{constants.ACCESS, constants.DOLLAR_DOLLAR + constants.BUCKET + constants.DOT + constants.ACCESS},
						{constants.NUMBER_OF_OBJECTS, constants.DOLLAR_DOLLAR + constants.BUCKET + constants.DOT + constants.NUMBER_OF_OBJECTS},
						{constants.TOTAL_SIZE, constants.DOLLAR_DOLLAR + constants.BUCKET + constants.DOT + constants.TOTAL_SIZE},
						{constants.TAGS, constants.DOLLAR_DOLLAR + constants.BUCKET + constants.DOT + constants.TAGS},
						//* asking to filter the objects array based on the object level queries given by user
						{constants.OBJECTS, bson.D{
							{constants.FILTER_AGG_OP, bson.D{
								{constants.INPUT, constants.DOLLAR_DOLLAR + constants.BUCKET + constants.DOT + constants.OBJECTS},
								{constants.AS, constants.OBJECT},
								{constants.COND_OPERATOR, bson.D{{constants.AND_OPERATOR, filterConditions}}},
							},
							},
						},
						},
					},
					},
				},
				},
			},
			},
		}}}
		objectLevelAggOperations := []bson.D{
			findMatchingDocuments,
			filterOnlyRequiredObjects,
		}
		aggOperations = append(aggOperations, objectLevelAggOperations...)
	}

	return aggOperations
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

	if in.SizeOfBucketInBytes != 0 && in.BucketSizeOperator != "" {
		bucketSizeMatchingCondition := GetMatchingConditions(constants.TOTAL_SIZE, in.SizeOfBucketInBytes, in.BucketSizeOperator)
		bucketSizeFilterCondition := GetConditionForOperator(constants.BUCKET_TOTAL_SIZE, in.SizeOfBucketInBytes, in.BucketSizeOperator)
		filterConditions = append(filterConditions, bucketSizeFilterCondition)
		matchConditions = append(matchConditions, bucketSizeMatchingCondition)
	}

	if len(filterConditions) > 0 {
		findMatchingDocuments := bson.D{{constants.MATCH_AGG_OP, bson.D{{
			constants.BUCKETS, bson.D{{
				constants.ELEMMATCH_AGG_OPERATOR,
				bson.D(matchConditions),
			}},
		}}}}

		log.Debugln("matching query for bucket level ", findMatchingDocuments)
		filterOnlyRequiredObjects := bson.D{
			{constants.PROJECT_AGG_OP, bson.D{
				{constants.ID, constants.INCLUDE_FIELD},
				{constants.BACKEND_NAME, constants.INCLUDE_FIELD},
				{constants.REGION, constants.INCLUDE_FIELD},
				{constants.TYPE, constants.INCLUDE_FIELD},
				{constants.BUCKETS, bson.D{
					{constants.FILTER_AGG_OP, bson.D{
						{constants.INPUT, constants.DOLLAR_SYMBOL + constants.BUCKETS},
						{constants.AS, constants.BUCKET},
						{constants.COND_OPERATOR, bson.D{
							{constants.AND_OPERATOR, filterConditions},
						}},
					}},
				}},
			}},
		}
		bucketLevelAggOperations := []bson.D{
			findMatchingDocuments,
			filterOnlyRequiredObjects,
		}
		aggOperations = append(aggOperations, bucketLevelAggOperations...)
	}

	return aggOperations
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
