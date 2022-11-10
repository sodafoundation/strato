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

	"github.com/opensds/multi-cloud/api/pkg/common"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	"github.com/opensds/multi-cloud/s3/pkg/utils"
	pb "github.com/opensds/multi-cloud/s3/proto"
)

func (ad *adapter) CountObjects(ctx context.Context, in *pb.ListObjectsRequest, out *utils.ObjsCountInfo) S3Error {
	ss := ad.session
	c := ss.Database(DataBaseName).Collection(in.Bucket)

	filt := ""
	if in.Filter[common.KObjKey] != "" {
		filt = in.Filter[common.KObjKey]
	}

	m := bson.M{
		utils.DBKEY_OBJECTKEY:    bson.M{"$regex": filt},
		utils.DBKEY_INITFLAG:     bson.M{"$ne": "0"},
		utils.DBKEY_DELETEMARKER: bson.M{"$ne": "1"},
	}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return InternalError
	}

	q1 := bson.M{
		"$match": m,
	}

	q2 := bson.M{
		"$group": bson.M{
			"_id":   nil,
			"size":  bson.M{"$sum": "$size"},
			"count": bson.M{"$sum": 1},
		},
	}

	operations := []bson.M{q1, q2}
	pipe, err := c.Watch(ctx, operations)
	//pipe := c.Pipe(operations)
	var ret utils.ObjsCountInfo
	err = pipe.Decode(&ret)
	if err == nil {
		out.Count = ret.Count
		out.Size = ret.Size
		log.Infof("count objects of bucket[%s] successfully, count=%d, size=%d\n", in.Bucket, out.Count, out.Size)
	} else if err == mgo.ErrNotFound {
		out.Count = 0
		out.Size = 0
		log.Infof("count objects of bucket[%s] successfully, count=0, size=0\n", in.Bucket)
	} else {
		log.Errorf("count objects of bucket[%s] failed, err:%v\n", in.Bucket, err)
		return InternalError
	}

	return NoError
}
