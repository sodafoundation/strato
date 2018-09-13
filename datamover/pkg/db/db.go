package db

import (
	"github.com/opensds/go-panda/dataflow/pkg/type"
	"github.com/opensds/go-panda/datamover/pkg/db/drivers/mongo"
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

type DBAdapter interface {
	UpdateJob(job *_type.Job) error
}
