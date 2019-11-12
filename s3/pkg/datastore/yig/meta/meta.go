package meta

import (
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/config"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/meta/db/driver"
)

type MetaConfig struct {
	Dbcfg config.DatabaseConfig
}

type Meta struct {
	db driver.DB
}

func (m *Meta) Close() {
	m.db.Close()
}

func New(cfg MetaConfig) (*Meta, error) {
	db, err := driver.Open(cfg.Dbcfg)
	if err != nil {
		return nil, err
	}

	m := &Meta{
		db: db,
	}
	return m, nil
}
