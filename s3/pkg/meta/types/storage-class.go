package types

import (
	. "github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/pkg/utils"
)

type StorageClass int32

var (
	StorageClassIndexMap = map[StorageClass]string{
		utils.Tier1:   "STANDARD",
		utils.Tier99:  "STANDARD_IA",
		utils.Tier999: "GLACIER",
	}

	StorageClassStringMap = map[string]StorageClass{
		"STANDARD":    utils.Tier1,
		"STANDARD_IA": utils.Tier99,
		"GLACIER":     utils.Tier999,
	}
)

func (s StorageClass) ToString() string {
	return StorageClassIndexMap[s]
}

func MatchStorageClassIndex(storageClass string) (StorageClass, error) {
	if index, ok := StorageClassStringMap[storageClass]; ok {
		return index, nil
	} else {
		return 0, ErrInvalidStorageClass
	}
}
