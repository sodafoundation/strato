package meta

import (
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/log"
	"github.com/opensds/multi-cloud/s3/pkg/meta/db"
	"github.com/opensds/multi-cloud/s3/pkg/meta/db/drivers/tidb"
)

const (
	ENCRYPTION_KEY_LENGTH = 32 // 32 bytes for AES-"256"
)

type Meta struct {
	Db db.DBAdapter
	Logger *log.Logger
	Cache  MetaCache
}

func (m *Meta) Stop() {
	if m.Cache != nil {
		m.Cache.Close()
	}
}

func New(logger *log.Logger, myCacheType CacheType) *Meta {
	meta := Meta{
		Logger: logger,
		Cache:  newMetaCache(myCacheType),
	}
	meta.Db = tidbclient.NewTidbClient()
	return &meta
}
