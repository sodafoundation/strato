// Copyright 2020 The SODA Authors.
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
	"errors"
	"math"
	"sync"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/micro/go-micro/v2/metadata"
	log "github.com/sirupsen/logrus"

	"github.com/soda/multi-cloud/api/pkg/common"
	"github.com/soda/multi-cloud/block/pkg/model"
)

var adapter = &mongoAdapter{}
var mutex sync.Mutex
var DataBaseName = "multi-cloud"
var VolumeCollection = "volumes"

func Init(host string) *mongoAdapter {
	mutex.Lock()
	defer mutex.Unlock()

	if adapter.session != nil {
		return adapter
	}

	session, err := mgo.Dial(host)
	if err != nil {
		panic(err)
	}
	session.SetMode(mgo.Monotonic, true)
	adapter.session = session
	return adapter
}

func Exit() {
	adapter.session.Close()
}

type mongoAdapter struct {
	session *mgo.Session
	userID  string
}

// The implementation of Repository
func UpdateFilter(m bson.M, filter map[string]string) error {
	for k, v := range filter {
		m[k] = interface{}(v)
	}
	return nil
}

func UpdateContextFilter(ctx context.Context, m bson.M) error {
	// if context is admin, no need filter by tenantId.
	md, ok := metadata.FromContext(ctx)
	if !ok {
		log.Error("Get context failed")
		return errors.New("Get context failed")
	}

	isAdmin, _ := md[common.CTX_KEY_IS_ADMIN]
	if isAdmin != common.CTX_VAL_TRUE {
		tenantId, ok := md[common.CTX_KEY_TENANT_ID]
		if !ok {
			log.Error("Get tenantid failed")
			return errors.New("Get tenantid failed")
		}
		m["tenantid"] = tenantId
	}

	return nil
}

func (adapter *mongoAdapter) ListVolume(ctx context.Context, limit, offset int,
	query interface{}) ([]*model.Volume, error) {

	session := adapter.session.Copy()
	defer session.Close()

	if limit == 0 {
		limit = math.MinInt32
	}
	var volumes []*model.Volume

	m := bson.M{}
	UpdateFilter(m, query.(map[string]string))
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return nil, err
	}
	log.Infof("ListVolumes, limit=%d, offset=%d, m=%+v\n", limit, offset, m)

	err = session.DB(DataBaseName).C(VolumeCollection).Find(m).Skip(offset).Limit(limit).All(&volumes)
	if err != nil {
		return nil, err
	}

	return volumes, nil
}

func (adapter *mongoAdapter) GetVolume(ctx context.Context, id string) (*model.Volume,
	error) {
	session := adapter.session.Copy()
	defer session.Close()

	m := bson.M{"_id": bson.ObjectIdHex(id)}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return nil, err
	}

	var volume = &model.Volume{}
	collection := session.DB(DataBaseName).C(VolumeCollection)
	err = collection.Find(m).One(volume)
	if err != nil {
		return nil, err
	}
	return volume, nil
}

func (adapter *mongoAdapter) CreateVolume(ctx context.Context, volume *model.Volume) (*model.Volume, error) {
	session := adapter.session.Copy()
	defer session.Close()

	if volume.Id == "" {
		volume.Id = bson.NewObjectId()
	}

	err := session.DB(DataBaseName).C(VolumeCollection).Insert(volume)
	if err != nil {
		return nil, err
	}
	return volume, nil
}

func (adapter *mongoAdapter) UpdateVolume(ctx context.Context, volume *model.Volume) (*model.Volume, error) {
	session := adapter.session.Copy()
	defer session.Close()

	m := bson.M{"_id": volume.Id}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return nil, err
	}

	err = session.DB(DataBaseName).C(VolumeCollection).Update(m, volume)
	if err != nil {
		return nil, err
	}

	return volume, nil
}

func (adapter *mongoAdapter) DeleteVolume(ctx context.Context, id string) error {
	session := adapter.session.Copy()
	defer session.Close()

	m := bson.M{"_id": bson.ObjectIdHex(id)}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return err
	}

	return session.DB(DataBaseName).C(VolumeCollection).Remove(m)
}
