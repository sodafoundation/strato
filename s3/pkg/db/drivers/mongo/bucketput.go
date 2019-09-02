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
	log "github.com/sirupsen/logrus"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	pb "github.com/opensds/multi-cloud/s3/proto"
)

func (ad *adapter) CreateBucket(in *pb.Bucket) S3Error {
	ss := ad.s.Copy()
	defer ss.Close()
	out := pb.Bucket{}
	c := ss.DB(DataBaseName).C(BucketMD)
	err := c.Find(bson.M{"name": in.Name}).One(out)
	if err == mgo.ErrNotFound {
		err := c.Insert(&in)
		if err != nil {
			log.Info("Add bucket to database failed, err:%v\n", err)
			return InternalError
		}
	} else {
		log.Info("The bucket already exists")
		return BucketAlreadyExists
	}

	return NoError
}

func (ad *adapter) UpdateBucket(bucket *pb.Bucket) S3Error {
	//Check if the policy exist or not
	ss := ad.s.Copy()
	defer ss.Close()
	//Update database
	c := ss.DB(DataBaseName).C(BucketMD)
	err := c.Update(bson.M{"name": bucket.Name}, bucket)
	if err == mgo.ErrNotFound {
		log.Info("Update bucket failed: the specified bucket does not exist.")
		return NoSuchBucket
	} else if err != nil {
		log.Info("Update bucket in database failed, err: %v.\n", err)
		return InternalError
	}
	return NoError
}
