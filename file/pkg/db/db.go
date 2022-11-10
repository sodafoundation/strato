// Copyright 2020 The SODA Authors.
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

	"github.com/soda/multi-cloud/file/pkg/model"

	"github.com/soda/multi-cloud/file/pkg/db/drivers/mongo"
	"github.com/soda/multi-cloud/file/pkg/utils/config"
)

// DbAdapter is a global variable that controls database module.
var DbAdapter DBAdapter

// Init function can perform some initialization work of different databases.
func Init(db *config.Database) {
	switch db.Driver {
	case "mongodb":
		DbAdapter = mongo.Init(db.Endpoint)
		return
	default:
		fmt.Printf("Can't find database driver %s!\n", db.Driver)
	}
}

func Exit(db *config.Database) {
	switch db.Driver {
	case "mongodb":
		mongo.Exit()
		return
	default:
		fmt.Printf("Can't find database driver %s!\n", db.Driver)
	}
}

type DBAdapter interface {
	ListFileShare(ctx context.Context, limit, offset int, query interface{}) ([]*model.FileShare, error)
	GetFileShare(ctx context.Context, id string) (*model.FileShare, error)
	GetFileShareByName(ctx context.Context, id string) (*model.FileShare, error)
	CreateFileShare(ctx context.Context, fileshare *model.FileShare) (*model.FileShare, error)
	UpdateFileShare(ctx context.Context, fileshare *model.FileShare) (*model.FileShare, error)
	DeleteFileShare(ctx context.Context, id string) error
}
