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
	"math"
	"sync"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	c "github.com/opensds/multi-cloud/api/pkg/context"
	"github.com/opensds/multi-cloud/backend/pkg/model"
)

type mongoRepository struct {
	session *mgo.Session
}

var defaultDBName = "multi-cloud"
var defaultCollection = "backends"
var mutex sync.Mutex
var mongoRepo = &mongoRepository{}

func Init(host string) *mongoRepository {
	mutex.Lock()
	defer mutex.Unlock()

	if mongoRepo.session != nil {
		return mongoRepo
	}

	session, err := mgo.Dial(host)
	if err != nil {
		panic(err)
	}
	session.SetMode(mgo.Monotonic, true)
	mongoRepo.session = session
	return mongoRepo
}

func UpdateFilter(m bson.M, filter map[string]string) error {
	for k, v := range filter {
		m[k] = interface{}(v)
	}
	return nil
}

func UpdateContextFilter(m bson.M, ctx *c.Context) error {
	// if context is admin, no need filter by tenantId.
	if !ctx.IsAdmin {
		m["tenantId"] = ctx.TenantId
	}
	return nil
}

// The implementation of Repository
func (repo *mongoRepository) CreateBackend(ctx *c.Context, backend *model.Backend) (*model.Backend, error) {
	session := repo.session.Copy()
	defer session.Close()

	if backend.Id == "" {
		backend.Id = bson.NewObjectId()
	}
	backend.TenantId = ctx.TenantId

	err := session.DB(defaultDBName).C(defaultCollection).Insert(backend)
	if err != nil {
		return nil, err
	}
	return backend, nil
}

func (repo *mongoRepository) DeleteBackend(ctx *c.Context, id string) error {
	session := repo.session.Copy()
	defer session.Close()

	m := bson.M{"_id": bson.ObjectIdHex(id)}
	UpdateContextFilter(m, ctx)

	return session.DB(defaultDBName).C(defaultCollection).Remove(m)
}

func (repo *mongoRepository) UpdateBackend(ctx *c.Context, backend *model.Backend) (*model.Backend, error) {
	session := repo.session.Copy()
	defer session.Close()

	m := bson.M{"_id": backend.Id}
	UpdateContextFilter(m, ctx)

	err := session.DB(defaultDBName).C(defaultCollection).Update(m, backend)
	if err != nil {
		return nil, err
	}
	return backend, nil
}

func (repo *mongoRepository) GetBackend(ctx *c.Context, id string) (*model.Backend, error) {
	session := repo.session.Copy()
	defer session.Close()

	m := bson.M{"_id": bson.ObjectIdHex(id)}
	UpdateContextFilter(m, ctx)

	var backend = &model.Backend{}
	collection := session.DB(defaultDBName).C(defaultCollection)
	err := collection.Find(m).One(backend)
	if err != nil {
		return nil, err
	}
	return backend, nil
}

func (repo *mongoRepository) ListBackend(ctx *c.Context, limit, offset int, query interface{}) ([]*model.Backend, error) {

	session := repo.session.Copy()
	defer session.Close()

	if limit == 0 {
		limit = math.MinInt32
	}
	var backends []*model.Backend
	m := bson.M{}
	UpdateFilter(m, query.(map[string]string))
	UpdateContextFilter(m, ctx)
	err := session.DB(defaultDBName).C(defaultCollection).Find(m).Skip(offset).Limit(limit).All(&backends)
	if err != nil {
		return nil, err
	}
	return backends, nil
}

func (repo *mongoRepository) Close() {
	repo.session.Close()
}
