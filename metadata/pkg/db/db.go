// Copyright 2019 The OpenSDS Authors.
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

package db

import (
	"context"
	"fmt"

	"github.com/opensds/multi-cloud/metadata/pkg/db/drivers/mongo"
	"github.com/opensds/multi-cloud/metadata/pkg/model"
	"github.com/opensds/multi-cloud/metadata/pkg/utils/config"
	"go.mongodb.org/mongo-driver/bson"
)

// DbAdapter is a global variable that controls database module.
var DbAdapter DBAdapter

// Init function can perform some initialization work of different databases.
func Init(db *config.Database) {
	switch db.Driver {
	case "etcd":
		fmt.Printf("etcd is not implemented right now!")
		return
	case "mongodb":
		DbAdapter = mongo.Init(db.Endpoint)
		return
	default:
		fmt.Printf("Can't find database driver %s!\n", db.Driver)
	}
}

func Exit(db *config.Database) {
	switch db.Driver {
	case "etcd":
		fmt.Printf("etcd is not implemented right now!")
		return
	case "mongodb":
		mongo.Exit()
		return
	default:
		fmt.Printf("Can't find database driver %s!\n", db.Driver)
	}
}

type DBAdapter interface {
	CreateMetadata(ctx context.Context, metaBackend model.MetaBackend) error
	ListMetadata(ctx context.Context, query []bson.D) ([]*model.MetaBackend, error)
}
