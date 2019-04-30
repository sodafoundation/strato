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
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/micro/go-log"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	pb "github.com/opensds/multi-cloud/s3/proto"
)

func (ad *adapter) CreateObject(in *pb.Object) S3Error {
	ss := ad.s.Copy()
	defer ss.Close()
	out := pb.Object{}
	c := ss.DB(DataBaseName).C(in.BucketName)
	err := c.Find(bson.M{"objectkey": in.ObjectKey}).One(out)
	if err == mgo.ErrNotFound {
		err := c.Insert(&in)
		if err != nil {
			log.Log("Add object to database failed, err:%v\n", err)
			return InternalError
		}
	} else if err != nil {
		return InternalError
	}

	return NoError
}

func (ad *adapter) UpdateObject(in *pb.Object) S3Error {
	ss := ad.s.Copy()
	defer ss.Close()
	c := ss.DB(DataBaseName).C(in.BucketName)
	err := c.Update(bson.M{"objectkey": in.ObjectKey}, in)
	if err == mgo.ErrNotFound {
		log.Log("Update object to database failed, err:%v\n", err)
		return NoSuchObject
	} else if err != nil {
		log.Log("Update object to database failed, err:%v\n", err)
		return InternalError
	}

	return NoError
}

func (ad *adapter) UpdateObjMeta(objKey *string, bucketName *string, setting map[string]interface{}) S3Error {
	ss := ad.s.Copy()
	defer ss.Close()
	c := ss.DB(DataBaseName).C(*bucketName)
	selector := bson.M{"objectkey":objKey}
	sets := []bson.M{}
	for k, v := range setting {
		sets = append(sets, bson.M{k:v})
	}
	data := bson.M{"$set":sets}
	err := c.Update(selector, data)

	if err != nil {
		log.Logf("Update object metadata failed:%v.\n", err)
		return DBError
	}

	return NoError
}