// Copyright (c) 2018 Huawei Technologies Co., Ltd. All Rights Reserved.
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

package pkg

import (
	"context"
	"os"

	"github.com/micro/go-log"
	. "github.com/opensds/multi-cloud/dataflow/pkg/utils"
	"github.com/opensds/multi-cloud/s3/pkg/db"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	pb "github.com/opensds/multi-cloud/s3/proto"
)

type s3Service struct{}

func (b *s3Service) ListBuckets(ctx context.Context, in *pb.BaseRequest, out *pb.ListBucketsResponse) error {
	log.Log("ListBuckets is called in s3 service.")
	buckets := []pb.Bucket{}
	err := db.DbAdapter.ListBuckets(in, &buckets)
	if err.Code != ERR_OK {
		return err.Error()
	}
	for j := 0; j < len(buckets); j++ {
		out.Buckets = append(out.Buckets, &buckets[j])
	}

	return nil
}

func (b *s3Service) CreateBucket(ctx context.Context, in *pb.Bucket, out *pb.BaseResponse) error {
	log.Log("CreateBucket is called in s3 service.")
	err := db.DbAdapter.CreateBucket(in)

	if err.Code != ERR_OK {
		return err.Error()
	}
	out.Msg = "Create bucket successfully."
	return nil
}

func (b *s3Service) GetBucket(ctx context.Context, in *pb.Bucket, out *pb.Bucket) error {
	log.Logf("GetBucket %s is called in s3 service.", in.Name)

	err := db.DbAdapter.GetBucketByName(in.Name, out)

	if err.Code != ERR_OK {
		return err.Error()
	}

	return nil
}

func (b *s3Service) DeleteBucket(ctx context.Context, in *pb.Bucket, out *pb.BaseResponse) error {
	log.Log("DeleteBucket is called in s3 service.")
	err := db.DbAdapter.DeleteBucket(in.Name)

	if err.Code != ERR_OK {
		return err.Error()
	}
	out.Msg = "Delete bucket successfully."

	return nil
}

func (b *s3Service) ListObjects(ctx context.Context, in *pb.ListObjectsRequest, out *pb.ListObjectResponse) error {
	log.Log("ListObject is called in s3 service.")
	objects := []pb.Object{}
	err := db.DbAdapter.ListObjects(in, &objects)

	if err.Code != ERR_OK {
		return err.Error()
	}
	for j := 0; j < len(objects); j++ {
		out.ListObjects = append(out.ListObjects, &objects[j])
	}
	return nil
}

func (b *s3Service) CreateObject(ctx context.Context, in *pb.Object, out *pb.BaseResponse) error {
	log.Log("PutObject is called in s3 service.")
	err := db.DbAdapter.CreateObject(in)

	if err.Code != ERR_OK {
		return err.Error()
	}
	out.Msg = "Create object successfully."

	return nil
}

func (b *s3Service) UpdateObject(ctx context.Context, in *pb.Object, out *pb.BaseResponse) error {
	log.Log("PutObject is called in s3 service.")
	out.Msg = "Create bucket successfully."
	return nil
}

func (b *s3Service) GetObject(ctx context.Context, in *pb.GetObjectInput, out *pb.Object) error {
	log.Log("GetObject is called in s3 service.")
	err := db.DbAdapter.GetObject(in, out)
	if err.Code != ERR_OK {
		return err.Error()
	}
	return nil
}

func (b *s3Service) DeleteObject(ctx context.Context, in *pb.DeleteObjectInput, out *pb.BaseResponse) error {
	log.Log("DeleteObject is called in s3 service.")
	err := db.DbAdapter.DeleteObject(in)
	if err.Code != ERR_OK {
		return err.Error()
	}
	out.Msg = "Delete object successfully."
	return nil
}

func NewS3Service() pb.S3Handler {
	host := os.Getenv("DB_HOST")
	dbstor := Database{Credential: "unkonwn", Driver: "mongodb", Endpoint: host}
	db.Init(&dbstor)
	return &s3Service{}
}
