package types


import . "github.com/opensds/multi-cloud/s3/error"


type StorageClass uint8

// Reference：https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/storage-class-intro.html
// to be compatible all kind of backend, we only support three types of storage class STANDARD,  STANDARD_IA, GLACIER
const (
	// ObjectStorageClassStandard is a ObjectStorageClass enum value
	ObjectStorageClassStandard StorageClass = iota

	// ObjectStorageClassGlacier is a ObjectStorageClass enum value
	ObjectStorageClassGlacier

	// ObjectStorageClassStandardIa is a ObjectStorageClass enum value
	ObjectStorageClassStandardIa
)

var (
	StorageClassIndexMap = map[StorageClass]string{
		ObjectStorageClassStandard:           "STANDARD",
		ObjectStorageClassStandardIa:         "STANDARD_IA",
		ObjectStorageClassGlacier:            "GLACIER",
	}

	StorageClassStringMap = map[string]StorageClass{
		"STANDARD":            ObjectStorageClassStandard,
		"STANDARD_IA":         ObjectStorageClassStandardIa,
		"GLACIER":             ObjectStorageClassGlacier,
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
