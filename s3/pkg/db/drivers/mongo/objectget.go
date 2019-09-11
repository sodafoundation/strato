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
	"github.com/micro/go-log"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	. "github.com/opensds/multi-cloud/s3/pkg/utils"
	pb "github.com/opensds/multi-cloud/s3/proto"
)

func (ad *adapter) GetObject(ctx context.Context, in *pb.GetObjectInput, out *pb.Object) S3Error {
	ss := ad.s.Copy()
	defer ss.Close()
	log.Log("Find object from database...... \n")

	m := bson.M{DBKEY_OBJECTKEY: in.Key}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return InternalError
	}

	err = ss.DB(DataBaseName).C(in.Bucket).Find(m).One(&out)
	if err == mgo.ErrNotFound {
		log.Log("object does not exist.")
		return NoSuchObject
	} else if err != nil {
		log.Log("find object from database failed, err:%v\n", err)
		return InternalError
	}

	return NoError
}
