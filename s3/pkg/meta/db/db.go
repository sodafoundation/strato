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
	pb "github.com/opensds/multi-cloud/s3/proto"
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
	UpdateObject4Lifecycle(ctx context.Context, old, new *Object, tx interface{}) error
	UpdateObjectMeta(object *Object) error

	//multipart
	CreateMultipart(multipart Multipart) (err error)
	GetMultipart(bucketName, objectName, uploadId string) (multipart Multipart, err error)
	DeleteMultipart(multipart *Multipart, tx interface{}) (err error)
	ListMultipartUploads(input *pb.ListBucketUploadRequest) (output *pb.ListBucketUploadResult, err error)
	PutObjectPart(multipart *Multipart, part *Part, tx interface{}) (err error)

	//bucket
	GetBucket(ctx context.Context, bucketName string) (bucket *Bucket, err error)
	GetBuckets(ctx context.Context) (buckets []*Bucket, err error)
	PutBucket(ctx context.Context, bucket *Bucket) error
	UpdateBucketSSE(ctx context.Context, bucketName string, sseType string) error
	CreateBucketSSE(ctx context.Context, bucketName string, sseType string) error
	CheckAndPutBucket(ctx context.Context, bucket *Bucket) (bool, error)
	DeleteBucket(ctx context.Context, bucket *Bucket) error
	ListObjects(ctx context.Context, bucketName string, versioned bool, maxKeys int, filter map[string]string) (
		retObjects []*Object, appendInfo utils.ListObjsAppendInfo, err error)

	CountObjects(ctx context.Context, bucketName, prefix string) (rsp *utils.ObjsCountInfo, err error)
	UpdateUsage(ctx context.Context, bucketName string, size int64, tx interface{}) error
	UpdateUsages(ctx context.Context, usages map[string]int64, tx interface{}) error
	ListBucketLifecycle(ctx context.Context) (bucket []*Bucket, err error)

	//gc
	PutGcobjRecord(ctx context.Context, object *Object, tx interface{}) error
	DeleteGcobjRecord(ctx context.Context, o *Object, tx interface{}) (err error)
	ListGcObjs(ctx context.Context, offset, limit int) ([]*Object, error)

	UpdateBucketVersioning(ctx context.Context, bucketName string, versionStatus string) error
	CreateBucketVersioning(ctx context.Context, bucketName string, versionStatus string) error
}
