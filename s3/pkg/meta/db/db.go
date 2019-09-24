package db

import (
	//"github.com/opensds/multi-cloud/api/pkg/s3/datatype"
	. "github.com/opensds/multi-cloud/s3/pkg/meta/types"
)

//DB Adapter Interface
type DBAdapter interface {
	//Transaction
	NewTrans() (tx interface{}, err error)
	AbortTrans(tx interface{}) error
	CommitTrans(tx interface{}) error

	//cluster
	GetCluster(fsid, pool string) (cluster Cluster, err error)
}