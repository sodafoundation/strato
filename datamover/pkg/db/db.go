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
	backend "github.com/opensds/multi-cloud/backend/pkg/model"
	"github.com/opensds/multi-cloud/dataflow/pkg/model"
	. "github.com/opensds/multi-cloud/dataflow/pkg/utils"
	"github.com/opensds/multi-cloud/datamover/pkg/db/drivers/mongo"
	log "github.com/sirupsen/logrus"
)

// C is a global variable that controls database module.
var DbAdapter DBAdapter

// Init function can perform some initialization work of different databases.
func Init(db *Database) {
	switch db.Driver {
	case "etcd":
		// C = etcd.Init(db.Driver, db.Crendential)
		log.Infof("etcd is not implemented right now!")
		return
	case "mongodb":
		//DbAdapter = mongo.Init(strings.Split(db.Endpoint, ","))
		DbAdapter = mongo.Init(db.Endpoint)
		return
	default:
		log.Infof("Can't find database driver %s!\n", db.Driver)
	}
}

func Exit(db *Database) {
	switch db.Driver {
	case "etcd":
		// C = etcd.Init(db.Driver, db.Crendential)
		log.Infof("etcd is not implemented right now!")
		return
	case "mongodb":
		mongo.Exit()
		return
	default:
		log.Infof("Can't find database driver %s!\n", db.Driver)
	}
}

type DBAdapter interface {
	GetJobStatus(jobID string) string
	UpdateJob(job *model.Job) error
	GetBackendByName(name string) (*backend.Backend, error)
	UpdateStatus(jobID string, jobStatus string) error
}
