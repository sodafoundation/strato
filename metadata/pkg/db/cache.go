package db

import (
	"context"
	"fmt"
	"github.com/opensds/multi-cloud/metadata/pkg/db/drivers/keydb"
	"github.com/opensds/multi-cloud/metadata/pkg/model"
	"github.com/opensds/multi-cloud/metadata/pkg/utils/config"
)

var Cacheadapter CacheAdapter

// Init function can perform some initialization work of different databases.
func InitCache(db *config.Database) {
	switch db.Driver {
	case "keydb":
		Cacheadapter = keydb.InitCache(db.Endpoint)
		return
	default:
		fmt.Printf("Can't find database driver %s!\n", db.Driver)
	}
}

func ExitCache(db *config.Database) {
	switch db.Driver {
	case "keydb":
		keydb.ExitCache()
		return
	default:
		fmt.Printf("Can't find database driver %s!\n", db.Driver)
	}
}

type CacheAdapter interface {
	StoreData(ctx context.Context, backends interface{}, query interface{}) error
	FetchData(ctx context.Context, query interface{}) ([]*model.MetaBackend, error)
}
