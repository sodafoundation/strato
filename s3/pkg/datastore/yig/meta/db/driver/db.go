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

	// delete multipart uploaded part objects and put them into gc
	PutPartsInGc(parts []*types.PartInfo) error

	// delete objects
	PutGcObjects(objects ...*types.GcObject) error

	// get gc objects by marker and limit
	GetGcObjects(marker int64, limit int) ([]*types.GcObject, error)

	// delete gc objects meta.
	DeleteGcObjects(objects ...*types.GcObject) error

	// close this driver if it is not needed.
	// Caution that this driver should be closed if the main process finishes.
	Close()
}
