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

func (ad *adapter) DeleteBucketLifecycle(in *pb.DeleteLifecycleInput) S3Error {
	//Check if the connector exist or not
	ss := ad.s.Copy()
	defer ss.Close()

	//Delete it from database
	c := ss.DB(DataBaseName).C(BucketMD)
	log.Infof("bucketName is %v:", in.Bucket)
	err := c.Update(bson.M{"name": in.Bucket}, bson.M{"$pull": bson.M{"lifecycleconfiguration": bson.M{"id": in.RuleID}}})
	if err != nil {
		log.Errorf("delete lifecycle for bucket : %s and lifecycle ruleID : %s failed,err:%v.\n", in.Bucket, in.RuleID, err)
		return NoSuchBucket
	} else {
		log.Infof("delete bucket lifecycle with rule id %s from database successfully", in.RuleID)
		return NoError
	}
}
