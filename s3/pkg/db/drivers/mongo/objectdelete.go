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

func (ad *adapter) DeleteObject(in *pb.DeleteObjectInput) S3Error {
	//Check if the connctor exist or not
	ss := ad.s.Copy()
	defer ss.Close()

	//Delete it from database
	c := ss.DB(DataBaseName).C(in.Bucket)
	_, err := c.RemoveAll(bson.M{"objectkey": in.Key})
	if err == mgo.ErrNotFound {
		log.Infof("Delete object %s failed, err:the specified object does not exist.", in.Key)
		return NoSuchObject
	} else if err != nil {
		log.Info("Delete object %s from database failed,err:%v.\n", in.Key, err)
		return InternalError
	}
	return NoError
}
