package driver

import (
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/meta/types"
)

//DB Interface
//Error returned by those functions should be ErrDBError, ErrNoSuchKey or ErrInternalError
type DB interface {
	// get the info of the cluster
	GetCluster(fsid, pool string) (cluster types.Cluster, err error)

	// get the multiparts which belong to the uploadId.
	ListParts(uploadId uint64) ([]*types.PartInfo, error)

	// put part info.
	PutPart(partInfo *types.PartInfo) error

	// complete the parts
	CompleteParts(uploadId uint64, parts []*types.PartInfo) error

	// delete the parts
	DeleteParts(uploadId uint64) error

	// close this driver if it is not needed.
	// Caution that this driver should be closed if the main process finishes.
	Close()
}
