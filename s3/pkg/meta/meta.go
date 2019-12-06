package meta

import (
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/log"
	"github.com/opensds/multi-cloud/s3/pkg/meta/db"
	"github.com/opensds/multi-cloud/s3/pkg/meta/db/drivers/tidb"
)

const (
	ENCRYPTION_KEY_LENGTH = 32 // 32 bytes for AES-"256"
)

type MetaConfig struct {
	CacheType     CacheType
	TidbInfo      string
}

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

func New(cfg MetaConfig) *Meta {
	meta := Meta{
		Cache:  newMetaCache(cfg.CacheType),
	}
	meta.Db = tidbclient.NewTidbClient(cfg.TidbInfo)
	return &meta
}
