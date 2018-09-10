package db

import (
	"github.com/opensds/go-panda/dataflow/pkg/type"
	"fmt"
	"github.com/opensds/go-panda/dataflow/pkg/db/drivers/mongo"
	. "github.com/opensds/go-panda/dataflow/pkg/utils"
)

// C is a global variable that controls database module.
var DbAdapter DBAdapter

// Init function can perform some initialization work of different databases.
func Init(db *Database) {
	switch db.Driver {
	case "etcd":
		// C = etcd.Init(db.Driver, db.Crendential)
		fmt.Printf("etcd is not implemented right now!")
		return
	case "mongodb":
		//DbAdapter = mongo.Init(strings.Split(db.Endpoint, ","))
		DbAdapter = mongo.Init(db.Endpoint)
		return
	default:
		fmt.Printf("Can't find database driver %s!\n", db.Driver)
	}
}

func Exit(db *Database){
	switch db.Driver {
	case "etcd":
		// C = etcd.Init(db.Driver, db.Crendential)
		fmt.Printf("etcd is not implemented right now!")
		return
	case "mongodb":
		mongo.Exit()
		return
	default:
		fmt.Printf("Can't find database driver %s!\n", db.Driver)
	}
}

func TestClear() error{
	err := mongo.TestClear()
	return err
}

type DBAdapter interface {
	//Policy
	CreatePolicy(pol *_type.Policy) _type.ErrCode
	DeletePolicy(id string, tenant string) _type.ErrCode
	UpdatePolicy(pol *_type.Policy) _type.ErrCode
	GetPolicy(name string, tenant string) ([]_type.Policy,_type.ErrCode)
	GetPolicyById(id string, tenant string)(*_type.Policy,_type.ErrCode)
	//Connector
	CreateConnector(conn *_type.Connector) _type.ErrCode
	DeleteConnector(name string, tenant string) _type.ErrCode
	UpdateConnector(conn *_type.Connector) _type.ErrCode
	GetConnector(name string, tenant string) ([]_type.Connector, _type.ErrCode)
	GetConnectorById(id string, tenant string) (*_type.Connector, _type.ErrCode)
	//Plan
	CreatePlan(conn *_type.Plan) _type.ErrCode
	DeletePlan(name string, tenant string) _type.ErrCode
	UpdatePlan(conn *_type.Plan) _type.ErrCode
	GetPlan(name string, tenant string) ([]_type.Plan,_type.ErrCode)
	GetPlanByid(id string, tenant string) (*_type.Plan, _type.ErrCode)
    LockSched(planId string) int
	UnlockSched(planId string) int
	//Job
	CreateJob(job *_type.Job) _type.ErrCode
	GetJob(id string, tenant string) ([]_type.Job,_type.ErrCode)
}
