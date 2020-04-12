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
	"strings"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	. "github.com/opensds/multi-cloud/s3/pkg/utils"
	log "github.com/sirupsen/logrus"
)

func (ad *adapter) DeleteBucket(ctx context.Context, bucketName string) S3Error {
	//Check if the connctor exist or not
	ss := ad.s.Copy()
	defer ss.Close()

	log.Infof("delete bucket, bucketName is %v:", bucketName)

	m := bson.M{DBKEY_NAME: bucketName}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return InternalError
	}

	//Delete it from database
	err = ss.DB(DataBaseName).C(BucketMD).Remove(m)
	log.Infof("err is %v:", err)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			log.Error("delete bucket from database failed, err: not found.")
			return NoSuchBucket
		} else {
			log.Infof("delete bucket from database failed, err: %v.\n", err.Error())
			return DBError
		}
	} else {
		log.Infof("Delete bucket from database successfully")
		return NoError
	}

	deleteErr := ss.DB(DataBaseName).C(bucketName).DropCollection()
	if deleteErr != nil && deleteErr != mgo.ErrNotFound {
		log.Errorf("delete bucket collection from database failed, err: %v.\n", deleteErr)
		return InternalError
	}

	return NoError
}
