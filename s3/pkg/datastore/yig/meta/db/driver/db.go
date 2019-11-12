package driver

import (
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/meta/types"
)

//DB Interface
//Error returned by those functions should be ErrDBError, ErrNoSuchKey or ErrInternalError
type DB interface {
	// get the info of the cluster
	GetCluster(fsid, pool string) (cluster types.Cluster, err error)

	// close this driver if it is not needed.
	// Caution that this driver should be closed if the main process finishes.
	Close()
}
