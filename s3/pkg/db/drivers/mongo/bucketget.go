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

func (ad *adapter) GetBucketByName(ctx context.Context, bucketName string, out *pb.Bucket) S3Error {
	ss := ad.s.Copy()
	defer ss.Close()

	log.Infof("GetBucketByName: bucketName %s", bucketName)

	m := bson.M{DBKEY_NAME: bucketName}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return InternalError
	}

	err = ss.DB(DataBaseName).C(BucketMD).Find(m).One(out)
	if err == mgo.ErrNotFound {
		log.Error("bucket does not exist.")
		return NoSuchBucket
	} else if err != nil {
		log.Errorf("get bucket from database failed, err: %v.\n", err)
		return InternalError
	}

	return NoError
}
