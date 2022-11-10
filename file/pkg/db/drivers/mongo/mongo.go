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
	"github.com/soda/multi-cloud/file/pkg/model"
)

var adapter = &mongoAdapter{}
var mutex sync.Mutex
var DataBaseName = "multi-cloud"
var FileShareCollection = "fileshares"

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

func (adapter *mongoAdapter) ListFileShare(ctx context.Context, limit, offset int,
	query interface{}) ([]*model.FileShare, error) {

	session := adapter.session.Copy()
	defer session.Close()

	if limit == 0 {
		limit = math.MinInt32
	}
	var fileshares []*model.FileShare

	m := bson.M{}
	UpdateFilter(m, query.(map[string]string))
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return nil, err
	}
	log.Infof("ListFileShares, limit=%d, offset=%d, m=%+v\n", limit, offset, m)

	err = session.DB(DataBaseName).C(FileShareCollection).Find(m).Skip(offset).Limit(limit).All(&fileshares)
	if err != nil {
		return nil, err
	}

	return fileshares, nil
}

func (adapter *mongoAdapter) GetFileShare(ctx context.Context, id string) (*model.FileShare,
	error) {
	session := adapter.session.Copy()
	defer session.Close()

	m := bson.M{"_id": bson.ObjectIdHex(id)}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return nil, err
	}

	var fileshare = &model.FileShare{}
	collection := session.DB(DataBaseName).C(FileShareCollection)
	err = collection.Find(m).One(fileshare)
	if err != nil {
		return nil, err
	}
	return fileshare, nil
}

func (adapter *mongoAdapter) GetFileShareByName(ctx context.Context, name string) (*model.FileShare,
	error) {
	session := adapter.session.Copy()
	defer session.Close()

	m := bson.M{"name": name}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return nil, err
	}

	var fileshare = &model.FileShare{}
	collection := session.DB(DataBaseName).C(FileShareCollection)
	err = collection.Find(m).One(fileshare)
	if err != nil {
		return nil, err
	}
	return fileshare, nil
}

func (adapter *mongoAdapter) CreateFileShare(ctx context.Context, fileshare *model.FileShare) (*model.FileShare, error) {
	session := adapter.session.Copy()
	defer session.Close()

	if fileshare.Id == "" {
		fileshare.Id = bson.NewObjectId()
	}

	err := session.DB(DataBaseName).C(FileShareCollection).Insert(fileshare)
	if err != nil {
		return nil, err
	}
	return fileshare, nil
}

func (adapter *mongoAdapter) DeleteFileShare(ctx context.Context, id string) error {
	session := adapter.session.Copy()
	defer session.Close()

	m := bson.M{"_id": bson.ObjectIdHex(id)}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return err
	}

	return session.DB(DataBaseName).C(FileShareCollection).Remove(m)
}

func (adapter *mongoAdapter) UpdateFileShare(ctx context.Context, fs *model.FileShare) (*model.FileShare, error) {
	session := adapter.session.Copy()
	defer session.Close()

	m := bson.M{"_id": fs.Id}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return nil, err
	}

	err = session.DB(DataBaseName).C(FileShareCollection).Update(m, fs)
	if err != nil {
		return nil, err
	}

	return fs, nil
}
