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
	"github.com/globalsign/mgo/bson"
	log "github.com/sirupsen/logrus"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	pb "github.com/opensds/multi-cloud/s3/proto"
)

func (ad *adapter) ListBuckets(in *pb.BaseRequest, out *[]pb.Bucket) S3Error {
	ss := ad.s.Copy()
	defer ss.Close()
	c := ss.DB(DataBaseName).C(BucketMD)

	log.Info("Find buckets from database...... \n")

	err := c.Find(bson.M{"owner": in.Id}).All(out)
	if err != nil {
		log.Info("Find buckets from database failed, err:%v\n", err)
		return InternalError
	}

	return NoError
}
