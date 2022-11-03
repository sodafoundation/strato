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

package mongo

import (
	"context"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	log "github.com/sirupsen/logrus"

	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	. "github.com/opensds/multi-cloud/s3/pkg/utils"
	pb "github.com/opensds/multi-cloud/s3/proto"
)

func (ad *adapter) CreateObject(ctx context.Context, in *pb.Object) S3Error {
	ss := ad.session

	m := bson.M{DBKEY_OBJECTKEY: in.ObjectKey}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return InternalError
	}

	out := pb.Object{}
	err = ss.Database(DataBaseName).Collection(in.BucketName).FindOne(ctx, m).Decode(out)
	if err == mgo.ErrNotFound {
		_, err := ss.Database(DataBaseName).Collection(in.BucketName).InsertOne(ctx, &in)
		if err != nil {
			log.Info("add object to database failed, err:%v\n", err)
			return InternalError
		}
	} else if err != nil {
		return InternalError
	}

	return NoError
}

func (ad *adapter) UpdateObject(ctx context.Context, in *pb.Object) S3Error {
	ss := ad.session

	m := bson.M{DBKEY_OBJECTKEY: in.ObjectKey}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return InternalError
	}

	log.Infof("update object:%+v\n", *in)
	_, err = ss.Database(DataBaseName).Collection(in.BucketName).UpdateOne(ctx, m, in)
	if err == mgo.ErrNotFound {
		log.Info("update object to database failed, err:%v\n", err)
		return NoSuchObject
	} else if err != nil {
		log.Info("update object to database failed, err:%v\n", err)
		return InternalError
	}

	return NoError
}

func (ad *adapter) UpdateObjMeta(ctx context.Context, objKey *string, bucketName *string, lastmod int64,
	setting map[string]interface{}) S3Error {
	ss := ad.session

	log.Infof("update object metadata: key=%s, bucket=%s, lastmodified=%d\n", *objKey, *bucketName, lastmod)

	m := bson.M{DBKEY_OBJECTKEY: *objKey, DBKEY_LASTMODIFIED: lastmod}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return InternalError
	}

	data := bson.M{"$set": setting}
	_, err = ss.Database(DataBaseName).Collection(*bucketName).UpdateOne(ctx, m, data)
	if err != nil {
		log.Errorf("update object[key=%s] metadata failed:%v.\n", *objKey, err)
		return DBError
	}

	return NoError
}
