package tidb

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"

	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/config"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/meta/db/driver"
)

const (
	TIDB_DRIVER_ID = "tidb"
)

type TidbDriver struct {
}

func (td *TidbDriver) OpenDB(dbCfg config.DatabaseConfig) (driver.DB, error) {
	db := &Tidb{}
	var err error
	db.DB, err = sql.Open("mysql", dbCfg.DbUrl)
	if err != nil {
		log.Errorf("failed to open db: %s, err: %v", dbCfg.DbUrl, err)
		return nil, err
	}
	log.Info("connected to tidb ...")
	db.DB.SetMaxIdleConns(dbCfg.MaxIdleConns)
	db.DB.SetMaxOpenConns(dbCfg.MaxOpenConns)
	return db, nil
}

func init() {
	tidbDriver := &TidbDriver{}
	driver.RegisterDBDriver(TIDB_DRIVER_ID, tidbDriver)
}
