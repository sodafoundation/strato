package datastore

import (
	"context"
	"fmt"
	"github.com/opensds/go-panda/api/pkg/s3/datastore/aliyun"
	"github.com/opensds/go-panda/api/pkg/s3/datastore/aws"
	"github.com/opensds/go-panda/api/pkg/s3/datastore/hws"
	backendpb "github.com/opensds/go-panda/backend/proto"
	. "github.com/opensds/go-panda/s3/pkg/exception"
	pb "github.com/opensds/go-panda/s3/proto"
	"io"
)

// Init function can perform some initialization work of different datastore.
func Init(backend *backendpb.BackendDetail) DataStoreAdapter {
	var StoreAdapter DataStoreAdapter

	switch backend.Type {
	case "aliyun":
		//DbAdapter = mongo.Init(strings.Split(db.Endpoint, ","))
		StoreAdapter = aliyun.Init(backend)
		return StoreAdapter
	case "obs":
		//DbAdapter = mongo.Init(strings.Split(db.Endpoint, ","))
		StoreAdapter = hws.Init(backend)
		return StoreAdapter
	case "aws":
		//DbAdapter = mongo.Init(strings.Split(db.Endpoint, ","))
		StoreAdapter = aws.Init(backend)
		return StoreAdapter
	default:
		fmt.Printf("Can't find datastore driver %s!\n", backend.Type)
	}
	return nil
}

func Exit(backendType string) {

}

type DataStoreAdapter interface {
	PUT(stream io.Reader, object *pb.Object, context context.Context) S3Error
	DELETE(object *pb.DeleteObjectInput, context context.Context) S3Error
}
