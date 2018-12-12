// Copyright (c) 2018 Huawei Technologies Co., Ltd. All Rights Reserved.
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
	"fmt"

	c "github.com/opensds/multi-cloud/api/pkg/context"
	"github.com/opensds/multi-cloud/backend/pkg/db/drivers/mongo"
	"github.com/opensds/multi-cloud/backend/pkg/model"
	"github.com/opensds/multi-cloud/backend/pkg/utils/config"
)

type Repository interface {
	// Backend
	CreateBackend(ctx *c.Context, backend *model.Backend) (*model.Backend, error)
	DeleteBackend(ctx *c.Context, id string) error
	UpdateBackend(ctx *c.Context, backend *model.Backend) (*model.Backend, error)
	GetBackend(ctx *c.Context, id string) (*model.Backend, error)
	ListBackend(ctx *c.Context, limit, offset int, query interface{}) ([]*model.Backend, error)
	Close()
}

var Repo Repository

func Init(db *config.Database) {
	switch db.Driver {
	case "etcd":
		// C = etcd.Init(db.Driver, db.Crendential)
		fmt.Printf("etcd is not implemented right now!")
		return
	case "mongodb":
		Repo = mongo.Init(db.Endpoint)
		return
	default:
		fmt.Printf("Can't find database driver %s!\n", db.Driver)
	}
}

func Exit() {
	Repo.Close()
}
