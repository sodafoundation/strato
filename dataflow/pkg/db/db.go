package db

import (
	"github.com/opensds/go-panda/dataflow/pkg/type"
	"github.com/opensds/go-panda/dataflow/pkg/db/drivers/mongo"
	. "github.com/opensds/go-panda/dataflow/pkg/utils"
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
