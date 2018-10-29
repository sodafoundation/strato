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
	"github.com/micro/go-log"
	c "github.com/opensds/multi-cloud/api/pkg/filters/context"
	"github.com/opensds/multi-cloud/dataflow/pkg/db/drivers/mongo"
	"github.com/opensds/multi-cloud/dataflow/pkg/model"
	. "github.com/opensds/multi-cloud/dataflow/pkg/utils"
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

func Exit(db *Database) {
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

func TestClear() error {
	err := mongo.TestClear()
	return err
}

type DBAdapter interface {
	//Policy
	CreatePolicy(ctx *c.Context, pol *model.Policy) (*model.Policy, error)
	DeletePolicy(ctx *c.Context, id string) error
	UpdatePolicy(ctx *c.Context, pol *model.Policy) (*model.Policy, error)
	ListPolicy(ctx *c.Context) ([]model.Policy, error)
	GetPolicy(ctx *c.Context, id string) (*model.Policy, error)
	//Plan
	CreatePlan(ctx *c.Context, conn *model.Plan) (*model.Plan, error)
	DeletePlan(ctx *c.Context, name string) error
	UpdatePlan(ctx *c.Context, conn *model.Plan) (*model.Plan, error)
	ListPlan(ctx *c.Context, limit int, offset int, filter interface{}) ([]model.Plan, error)
	GetPlan(ctx *c.Context, id string) (*model.Plan, error)
	GetPlanByPolicy(ctx *c.Context, policyId string, limit int, offset int) ([]model.Plan, error)
	LockSched(planId string) int
	UnlockSched(planId string) int
	//Job
	CreateJob(ctx *c.Context, job *model.Job) (*model.Job, error)
	GetJob(ctx *c.Context, id string) (*model.Job, error)
	ListJob(ctx *c.Context, limit int, offset int, filter interface{}) ([]model.Job, error)
}
