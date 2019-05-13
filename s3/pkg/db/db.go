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
	"fmt"

	. "github.com/opensds/multi-cloud/s3/pkg/utils"
	"github.com/opensds/multi-cloud/s3/pkg/db/drivers/mongo"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	pb "github.com/opensds/multi-cloud/s3/proto"
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

func Exit(db *Database) {
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
	ListBuckets(in *pb.BaseRequest, out *[]pb.Bucket) S3Error
	CreateObject(in *pb.Object) S3Error
	UpdateObject(in *pb.Object) S3Error
	DeleteObject(in *pb.DeleteObjectInput) S3Error
	GetObject(in *pb.GetObjectInput, out *pb.Object) S3Error
	ListObjects(in *pb.ListObjectsRequest, out *[]pb.Object) S3Error
	DeleteBucketLifecycle(in *pb.DeleteLifecycleInput) S3Error
	UpdateObjMeta(objKey *string, bucketName *string, setting map[string]interface{}) S3Error
}
