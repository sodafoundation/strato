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
	"encoding/json"
	"strconv"
	"time"

	"github.com/globalsign/mgo/bson"
	"github.com/micro/go-log"
	"github.com/opensds/multi-cloud/api/pkg/common"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	"github.com/opensds/multi-cloud/s3/pkg/utils"
	pb "github.com/opensds/multi-cloud/s3/proto"
)

func (ad *adapter) ListObjects(in *pb.ListObjectsRequest, out *[]pb.Object) S3Error {
	ss := ad.s.Copy()
	defer ss.Close()
	c := ss.DB(DataBaseName).C(in.Bucket)

	log.Log("Find objects from database...... \n")

	filter := []bson.M{}
	if in.Filter != nil {
		if in.Filter[common.KObjKey] != "" {
			filter = append(filter, bson.M{"objectkey": bson.M{"$regex": in.Filter[common.KObjKey]}})
		}
		if in.Filter[common.KLastModified] != "" {
			var tmFilter map[string]string
			err := json.Unmarshal([]byte(in.Filter[common.KLastModified]), &tmFilter)
			if err != nil {
				log.Logf("unmarshal lastmodified value failed:%s\n", err)
				return InvalidQueryParameter
			}
			for k, v := range tmFilter {
				ts, _ := strconv.Atoi(v)
				secs := time.Now().Unix() - int64(ts*24*60*60)
				var op string
				switch k {
				case "lt":
					op = "$gt"
				case "gt":
					op = "$lt"
				case "lte":
					op = "$gte"
				case "gte":
					op = "$lte"
				default:
					log.Logf("unsupport filter action:%s\n", k)
					return InvalidQueryParameter
				}
				filter = append(filter, bson.M{"lastmodified": bson.M{op: secs}})
			}
		}
		if in.Filter[common.KStorageTier] != "" {
			tier, err := strconv.Atoi(in.Filter[common.KStorageTier])
			if err != nil {
				log.Logf("invalid storage class:%s\n", in.Filter[common.KStorageTier])
				return InvalidQueryParameter
			}
			filter = append(filter, bson.M{"tier": bson.M{"$lte": tier}})
		}
	}

	filter = append(filter, bson.M{utils.DBKEY_INITFLAG: bson.M{"$ne": "0"}})
	filter = append(filter, bson.M{utils.DBKEY_DELETEMARKER: bson.M{"$ne": "1"}})

	log.Logf("filter:%+v\n", filter)
	var err error
	offset := int(in.Offset)
	limit := int(in.Limit)
	if limit == 0 {
		// as default
		limit = 1000
	}
	if len(filter) > 0 {
		err = c.Find(bson.M{"$and": filter}).Skip(offset).Limit(limit).All(out)
	} else {
		err = c.Find(bson.M{}).Skip(offset).Limit(limit).All(out)
	}

	if err != nil {
		log.Logf("find objects from database failed, err:%v\n", err)
		return InternalError
	}

	return NoError
}
