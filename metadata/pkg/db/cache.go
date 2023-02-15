package db

// Copyright 2023 The OpenSDS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
