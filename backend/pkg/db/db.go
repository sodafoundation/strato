package db

import (
	"fmt"

	"github.com/opensds/go-panda/backend/pkg/db/drivers/mongo"
	"github.com/opensds/go-panda/backend/pkg/model"
	"github.com/opensds/go-panda/backend/pkg/utils/config"
)

type Repository interface {
	// Backend
	CreateBackend(backend *model.Backend) (*model.Backend, error)
	DeleteBackend(id string) error
	UpdateBackend(backend *model.Backend) (*model.Backend, error)
	GetBackend(id string) (*model.Backend, error)
	ListBackend(limit, offset int, query interface{}) ([]*model.Backend, error)
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
