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

package mongo

import (
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/micro/go-log"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	"strings"
)

func (ad *adapter) DeleteBucket(bucketName string) S3Error {
	//Check if the connctor exist or not
	ss := ad.s.Copy()
	defer ss.Close()

	//Delete it from database
	c := ss.DB(DataBaseName).C(BucketMD)
	log.Logf("bucketName is %v:",bucketName)
	err := c.Remove(bson.M{"name": bucketName})
	log.Logf("err is %v:",err)
	if err != nil {
		if strings.Contains(err.Error(),"not found") {
			log.Logf("Delete bucket from database failed,err:%v.\n", err.Error())
			return NoSuchBucket
		}else{
			log.Logf("Delete bucket from database failed,err:%v.\n", err.Error())
			return DBError
		}

	} else {
		log.Logf("Delete bucket from database successfully")
		return NoError
	}
	cc := ss.DB(DataBaseName).C(bucketName)
	deleteErr := cc.DropCollection()
	if deleteErr != nil && deleteErr != mgo.ErrNotFound {
		log.Logf("Delete bucket collection from database failed,err:%v.\n", deleteErr)
		return InternalError
	}

	return NoError
}
