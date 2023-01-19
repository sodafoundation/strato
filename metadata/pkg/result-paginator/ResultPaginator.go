package result_paginator

import (
	"github.com/opensds/multi-cloud/metadata/pkg/model"
	"math"
)

func Paginate(unPaginatedResult []*model.MetaBackend, limit int32, offset int32) []*model.MetaBackend {
	lengthOfUnPaginatedResult := int32(len(unPaginatedResult))
	if offset >= lengthOfUnPaginatedResult {
		return []*model.MetaBackend{}
	} else {
		lastResultIndex := getLastResultIndex(limit, offset, lengthOfUnPaginatedResult)
		return unPaginatedResult[offset:lastResultIndex]
	}
}

func getLastResultIndex(limit int32, offset int32, lengthOfUnPaginatedResult int32) int {
	// Get the last index of the result item to be returned in the result
	minOfTwo := math.Min(float64(limit+offset), float64(lengthOfUnPaginatedResult))
	return int(minOfTwo)
}
