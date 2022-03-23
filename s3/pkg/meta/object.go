// Copyright 2019 The soda Authors.
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
package meta

import (
	"context"

	"strconv"

	log "github.com/sirupsen/logrus"

	. "github.com/soda/multi-cloud/s3/error"
	"github.com/soda/multi-cloud/s3/pkg/helper"
	tidbclient "github.com/soda/multi-cloud/s3/pkg/meta/db/drivers/tidb"
	"github.com/soda/multi-cloud/s3/pkg/meta/redis"
	. "github.com/soda/multi-cloud/s3/pkg/meta/types"
)

const (
	OBJECT_CACHE_PREFIX = "object:"
)

// Object will be updated to cache while willNeed is true
func (m *Meta) GetObject(ctx context.Context, bucketName string, objectName string, versionId string, willNeed bool) (object *Object, err error) {
	getObject := func() (o helper.Serializable, err error) {
		log.Info("GetObject CacheMiss. bucket:", bucketName, ", object:", objectName)
		version := ""
		if versionId != "" {
			version = strconv.FormatUint(tidbclient.VersionStr2UInt64(versionId), 10)
		}
		object, err := m.Db.GetObject(ctx, bucketName, objectName, version)
		if err != nil {
			log.Errorln("get object failed, err:", err)
			return
		}
		log.Infoln("GetObject object.Name:", objectName)
		if object.ObjectKey != objectName {
			err = ErrNoSuchKey
			return
		}
		return object, nil
	}

	toObject := func(fields map[string]string) (interface{}, error) {
		o := &Object{}
		return o.Deserialize(fields)
	}

	o, err := m.Cache.Get(redis.ObjectTable, OBJECT_CACHE_PREFIX, bucketName+":"+objectName+":",
		getObject, toObject, willNeed)
	if err != nil {
		return
	}
	object, ok := o.(*Object)
	if !ok {
		err = ErrInternalError
		return
	}
	return object, nil
}

func (m *Meta) PutObject(ctx context.Context, object, deleteObj *Object, multipart *Multipart, objMap *ObjMap, updateUsage bool) error {
	log.Debugf("PutObject begin, object=%+v, deleteObj:%+v\n", object, deleteObj)
	tx, err := m.Db.NewTrans()
	defer func() {
		if err != nil {
			m.Db.AbortTrans(tx)
		}
	}()

	// if target object exist and it's location is different from new location, need to clean it
	if deleteObj != nil {
		if deleteObj.Location != object.Location {
			log.Infoln("put gc, deleteObj:", deleteObj)
			err = m.Db.PutGcobjRecord(ctx, deleteObj, tx)
			if err != nil {
				return err
			}
		}

		log.Infoln("delete object metadata, deleteObj:", deleteObj)
		err = m.Db.DeleteObject(ctx, deleteObj, tx)
		if err != nil {
			return err
		}
	}

	err = m.Db.PutObject(ctx, object, tx)
	if err != nil {
		return err
	}

	if multipart != nil {
		err = m.Db.DeleteMultipart(multipart, tx)
		if err != nil {
			return err
		}
	}

	// TODO: usage need to be updated for charging, and it depends on redis, and the mechanism is:
	// 1. Update usage in redis when each put happens.
	// 2. Update usage in database periodically based on redis.
	// see https://github.com/soda/multi-cloud/issues/698 for redis related issue.

	err = m.Db.CommitTrans(tx)
	return nil
}

func (m *Meta) UpdateObjectMeta(object *Object) error {
	err := m.Db.UpdateObjectMeta(object)
	return err
}

func (m *Meta) DeleteObject(ctx context.Context, object *Object) error {
	tx, err := m.Db.NewTrans()
	defer func() {
		if err != nil {
			m.Db.AbortTrans(tx)
		}
	}()

	err = m.Db.DeleteObject(ctx, object, tx)
	if err != nil {
		return err
	}

	// TODO: usage need to be updated for charging, it depends on redis, and the mechanism is:
	// 1. Update usage in redis when each delete happens.
	// 2. Update usage in database periodically based on redis.
	// see https://github.com/soda/multi-cloud/issues/698 for redis related issue.

	err = m.Db.CommitTrans(tx)

	return err
}

func (m *Meta) MarkObjectAsDeleted(ctx context.Context, object *Object) error {
	return m.Db.SetObjectDeleteMarker(ctx, object, true)
}

func (m *Meta) UpdateObject4Lifecycle(ctx context.Context, old, new *Object, multipart *Multipart) (err error) {
	log.Infof("update object from %v to %v\n", *old, *new)
	tx, err := m.Db.NewTrans()
	defer func() {
		if err != nil {
			m.Db.AbortTrans(tx)
		}
	}()

	err = m.Db.UpdateObject4Lifecycle(ctx, old, new, tx)
	if err != nil {
		return err
	}

	if multipart != nil {
		err = m.Db.DeleteMultipart(multipart, tx)
		if err != nil {
			return err
		}
	}

	err = m.Db.CommitTrans(tx)
	return err
}
