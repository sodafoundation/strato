package db

import (
	"fmt"
	"github.com/opensds/go-panda/s3/pkg/db/drivers/mongo"
	pb "github.com/opensds/go-panda/s3/proto"
	. "github.com/opensds/go-panda/s3/pkg/exception"
	. "github.com/opensds/go-panda/dataflow/pkg/utils"
)

// DbAdapter is a global variable that controls database module.
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


type DBAdapter interface {
	CreateBucket(bucket *pb.Bucket) S3Error
	DeleteBucket(name string) S3Error
	UpdateBucket(bucket *pb.Bucket) S3Error
	GetBucketByName(name string, out *pb.Bucket) S3Error
}
