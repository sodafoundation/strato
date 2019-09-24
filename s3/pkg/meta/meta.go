package meta

import (
	"github.com/opensds/multi-cloud/s3/pkg/meta/db"
	"github.com/opensds/multi-cloud/s3/pkg/meta/db/drivers/tidb"
)

const (
	ENCRYPTION_KEY_LENGTH = 32 // 32 bytes for AES-"256"
)

type Meta struct {
	Db db.DBAdapter
	Cache  MetaCache
}

func (m *Meta) Stop() {
	if m.Cache != nil {
		m.Cache.Close()
	}
}

func New( myCacheType CacheType) *Meta {
	meta := Meta{
		Cache:  newMetaCache(myCacheType),
	}
	meta.Db = tidbclient.NewTidbClient()
	return &meta
}
