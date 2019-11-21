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

	. "github.com/opensds/multi-cloud/s3/pkg/meta/types"
	"github.com/opensds/multi-cloud/s3/pkg/utils"
)

//DB Adapter Interface
//Error returned by those functions should be ErrDBError, ErrNoSuchKey or ErrInternalError
type DBAdapter interface {
	//Transaction
	NewTrans() (tx interface{}, err error)
	AbortTrans(tx interface{}) error
	CommitTrans(tx interface{}) error
	//object
	GetObject(ctx context.Context, bucketName, objectName, version string) (object *Object, err error)
	//GetAllObject(bucketName, objectName, version string) (object []*Object, err error)
	PutObject(ctx context.Context, object *Object, tx interface{}) error
	DeleteObject(ctx context.Context, object *Object, tx interface{}) error
	SetObjectDeleteMarker(ctx context.Context, object *Object, deleteMarker bool) error

	//bucket
	GetBucket(ctx context.Context, bucketName string) (bucket *Bucket, err error)
	GetBuckets(ctx context.Context) (buckets []*Bucket, err error)
	PutBucket(ctx context.Context, bucket *Bucket) error
	CheckAndPutBucket(ctx context.Context, bucket *Bucket) (bool, error)
	DeleteBucket(ctx context.Context, bucket *Bucket) error
	ListObjects(ctx context.Context, bucketName string, versioned bool, maxKeys int, filter map[string]string) (
		retObjects []*Object, appendInfo utils.ListObjsAppendInfo, err error)

	UpdateUsage(ctx context.Context, bucketName string, size int64, tx interface{}) error
	UpdateUsages(ctx context.Context, usages map[string]int64, tx interface{}) error
	ListBucketLifecycle(ctx context.Context) (bucket []*Bucket, err error)

	UpdateBucketVersioning(ctx context.Context, bucketName string, versionStatus string) error
	CreateBucketVersioning(ctx context.Context, bucketName string, versionStatus string) error
}
