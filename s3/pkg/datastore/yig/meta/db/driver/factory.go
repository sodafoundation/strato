package driver

import (
	"errors"
	"fmt"
	"sync"

	"github.com/soda/multi-cloud/s3/pkg/datastore/yig/config"
)

var (
	driversLock = sync.RWMutex{}
	drivers     = make(map[string]DBDriver)
)

func Open(dbCfg config.DatabaseConfig) (DB, error) {
	driversLock.RLock()
	driver, ok := drivers[dbCfg.DbType]
	driversLock.RUnlock()
	if !ok {
		return nil, errors.New(fmt.Sprintf("unknow db driver %s", dbCfg.DbType))
	}
	return driver.OpenDB(dbCfg)
}

func RegisterDBDriver(dbType string, dbDriver DBDriver) {
	if dbDriver == nil {
		return
	}

	driversLock.Lock()
	defer driversLock.Unlock()
	drivers[dbType] = dbDriver
}
