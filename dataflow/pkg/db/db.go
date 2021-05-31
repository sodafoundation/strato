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

	"github.com/opensds/multi-cloud/dataflow/pkg/db/drivers/mongo"
	"github.com/opensds/multi-cloud/dataflow/pkg/model"
	. "github.com/opensds/multi-cloud/dataflow/pkg/utils"

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

func TestClear() error {
	err := mongo.TestClear()
	return err
}

type DBAdapter interface {
	//Policy
	CreatePolicy(ctx context.Context, pol *model.Policy) (*model.Policy, error)
	DeletePolicy(ctx context.Context, id string) error
	UpdatePolicy(ctx context.Context, pol *model.Policy) (*model.Policy, error)
	ListPolicy(ctx context.Context) ([]model.Policy, error)
	GetPolicy(ctx context.Context, id string) (*model.Policy, error)
	//Plan
	CreatePlan(ctx context.Context, conn *model.Plan) (*model.Plan, error)
	DeletePlan(ctx context.Context, name string) error
	UpdatePlan(ctx context.Context, conn *model.Plan) (*model.Plan, error)
	ListPlan(ctx context.Context, limit int, offset int, filter interface{}) ([]model.Plan, error)
	GetPlan(ctx context.Context, id string) (*model.Plan, error)
	GetPlanByPolicy(ctx context.Context, policyId string, limit int, offset int) ([]model.Plan, error)
	LockSched(tenantId, planId string) int
	UnlockSched(tenantId, planId string) int
	LockBucketLifecycleSched(bucketName string) int
	UnlockBucketLifecycleSched(bucketName string) int
	//Job
	CreateJob(ctx context.Context, job *model.Job) (*model.Job, error)
	GetJob(ctx context.Context, id string) (*model.Job, error)
	ListJob(ctx context.Context, limit int, offset int, filter interface{}) ([]model.Job, error)
}
