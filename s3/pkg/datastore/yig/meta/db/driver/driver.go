package driver

import (
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/config"
)

//DB Driver Interface
//Error returned by those functions should be ErrDBError, ErrNoSuchKey or ErrInternalError
type DBDriver interface {
	// initialize this driver
	OpenDB(dbCfg config.DatabaseConfig) (DB, error)
}
