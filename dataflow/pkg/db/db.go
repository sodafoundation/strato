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
	"github.com/opensds/multi-cloud/dataflow/pkg/type"
	"github.com/opensds/multi-cloud/dataflow/pkg/db/drivers/mongo"
	. "github.com/opensds/multi-cloud/dataflow/pkg/utils"
	"github.com/micro/go-log"
)

// C is a global variable that controls database module.
var DbAdapter DBAdapter

// Init function can perform some initialization work of different databases.
func Init(db *Database) {
	switch db.Driver {
	case "etcd":
		// C = etcd.Init(db.Driver, db.Crendential)
		log.Logf("etcd is not implemented right now!")
		return
	case "mongodb":
		//DbAdapter = mongo.Init(strings.Split(db.Endpoint, ","))
		DbAdapter = mongo.Init(db.Endpoint)
		return
	default:
		log.Logf("Can't find database driver %s!\n", db.Driver)
	}
}

func Exit(db *Database){
	switch db.Driver {
	case "etcd":
		// C = etcd.Init(db.Driver, db.Crendential)
		log.Logf("etcd is not implemented right now!")
		return
	case "mongodb":
		mongo.Exit()
		return
	default:
		log.Logf("Can't find database driver %s!\n", db.Driver)
	}
}

func TestClear() error{
	err := mongo.TestClear()
	return err
}

type DBAdapter interface {
	//Policy
	CreatePolicy(pol *_type.Policy) error
	DeletePolicy(id string, tenant string) error
	UpdatePolicy(pol *_type.Policy) error
	GetPolicy(name string, tenant string) ([]_type.Policy,error)
	GetPolicyById(id string, tenant string)(*_type.Policy,error)
	//Plan
	CreatePlan(conn *_type.Plan) error
	DeletePlan(name string, tenant string) error
	UpdatePlan(conn *_type.Plan) error
	GetPlan(name string, tenant string) ([]_type.Plan,error)
	GetPlanByid(id string, tenant string) (*_type.Plan, error)
    LockSched(planId string) int
	UnlockSched(planId string) int
	//Job
	CreateJob(job *_type.Job) error
	GetJob(id string, tenant string) ([]_type.Job,error)
}
