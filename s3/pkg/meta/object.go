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
package meta

import (
	"context"

	. "github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/pkg/helper"
	"github.com/opensds/multi-cloud/s3/pkg/meta/redis"
	. "github.com/opensds/multi-cloud/s3/pkg/meta/types"
	log "github.com/sirupsen/logrus"
)

const (
	OBJECT_CACHE_PREFIX = "object:"
)

func (m *Meta) GetObject(ctx context.Context, bucketName string, objectName string, willNeed bool) (object *Object, err error) {
	getObject := func() (o helper.Serializable, err error) {
		log.Info("GetObject CacheMiss. bucket:", bucketName, "object:", objectName)
		object, err := m.Db.GetObject(ctx, bucketName, objectName, "")
		if err != nil {
			return
		}
		log.Infoln("GetObject object.Name:", object.ObjectKey)
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

func (m *Meta) PutObject(ctx context.Context, object *Object, multipart *Multipart, objMap *ObjMap, updateUsage bool) error {
	tx, err := m.Db.NewTrans()
	defer func() {
		if err != nil {
			m.Db.AbortTrans(tx)
		}
	}()

	err = m.Db.PutObject(ctx, object, tx)
	if err != nil {
		return err
	}

	// TODO: usage need to be updated for charging, and it depends on redis, and the mechanism is:
	// 1. Update usage in redis when each put happens.
	// 2. Update usage in database periodically based on redis.
	// see https://github.com/opensds/multi-cloud/issues/698 for redis related issue.

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
	// see https://github.com/opensds/multi-cloud/issues/698 for redis related issue.

	err = m.Db.CommitTrans(tx)

	return err
}

func (m *Meta) MarkObjectAsDeleted(ctx context.Context, object *Object) error {
	return m.Db.SetObjectDeleteMarker(ctx, object, true)
}
