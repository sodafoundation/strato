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
	"errors"
	"math"
	"sync"

	"github.com/globalsign/mgo/bson"
	"github.com/micro/go-micro/v2/metadata"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/opensds/multi-cloud/api/pkg/common"
	"github.com/opensds/multi-cloud/backend/pkg/model"
)

type mongoRepository struct {
	session *mongo.Client
}

var defaultDBName = "multi-cloud"
var defaultCollection = "backends"
var defaultTierCollection = "tiers"
var mutex sync.Mutex
var mongoRepo = &mongoRepository{}
var mongodb = "mongodb://"

func Init(host string) *mongoRepository {
	mutex.Lock()
	defer mutex.Unlock()

	if mongoRepo.session != nil {
		return mongoRepo
	}
	// Create a new client and connect to the server
	uri := mongodb + host
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))
	if err != nil {
		panic(err)
	}
	if err := client.Ping(context.TODO(), readpref.Primary()); err != nil {
		panic(err)
	}
	log.Debugln("Successfully connected and pinged.")

	mongoRepo.session = client
	return mongoRepo
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
		log.Error("get context failed")
		return errors.New("get context failed")
	}

	isAdmin, _ := md[common.CTX_KEY_IS_ADMIN]
	if isAdmin != common.CTX_VAL_TRUE {
		tenantId, _ := md[common.CTX_REPRE_TENANT]
		if tenantId == "" {
			tenantId, ok = md[common.CTX_KEY_TENANT_ID]
			if !ok {
				log.Error("get tenantid failed")
				return errors.New("get tenantid failed")
			}
		}

		m["tenantId"] = tenantId
	}

	return nil
}

type session struct {
	sess *mongo.Client
}

func (repo *mongoRepository) CreateBackend(ctx context.Context, backend *model.Backend) (*model.Backend, error) {
	session := repo.session
	if backend.Id == "" {
		backend.Id = bson.NewObjectId()
	}

	_, err := session.Database(defaultDBName).Collection(defaultCollection).InsertOne(ctx, backend)

	if err != nil {
		return nil, err
	}
	return backend, nil
}

func (repo *mongoRepository) DeleteBackend(ctx context.Context, id string) error {
	session := repo.session
	m := bson.M{"_id": bson.ObjectIdHex(id)}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return err
	}

	_, err = session.Database(defaultDBName).Collection(defaultCollection).DeleteOne(ctx, m)
	return nil
}

func (repo *mongoRepository) UpdateBackend(ctx context.Context,
	backend *model.Backend) (*model.Backend, error) {
	session := repo.session
	m := bson.M{"_id": backend.Id}
	upd := bson.M{"$set": bson.M{"access": backend.Access, "secret": backend.Security}}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return nil, err
	}

	_, err = session.Database(defaultDBName).Collection(defaultCollection).UpdateOne(ctx, m, upd)
	if err != nil {
		return nil, err
	}

	return backend, nil
}

func (repo *mongoRepository) GetBackend(ctx context.Context, id string) (*model.Backend,
	error) {
	session := repo.session
	m := bson.M{"_id": bson.ObjectIdHex(id)}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return nil, err
	}

	var backend = &model.Backend{}
	collection := session.Database(defaultDBName).Collection(defaultCollection)
	err = collection.FindOne(ctx, m).Decode(&backend)
	if err != nil {
		return nil, err
	}
	return backend, nil
}

func (repo *mongoRepository) ListBackend(ctx context.Context, limit, offset int,
	query interface{}) ([]*model.Backend, error) {
	session := repo.session
	if limit == 0 {
		limit = math.MinInt32
	}
	var backends []*model.Backend

	m := bson.M{}
	UpdateFilter(m, query.(map[string]string))
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return nil, err
	}
	log.Infof("ListBackend, limit=%d, offset=%d, m=%+v\n", limit, offset, m)

	cur, err := session.Database(defaultDBName).Collection(defaultCollection).Find(ctx, m, options.Find().SetSkip(int64(offset)).SetLimit(int64(limit)))

	if err != nil {
		return nil, err
	}

	//Map result to slice
	for cur.Next(context.TODO()) {
		t := &model.Backend{}
		err := cur.Decode(&t)
		if err != nil {
			return backends, err
		}
		backends = append(backends, t)
	}

	return backends, nil
}

func (repo *mongoRepository) CreateTier(ctx context.Context, tier *model.Tier) (*model.Tier, error) {
	log.Debug("received request to create tier in db")

	session := repo.session
	if tier.Id == "" {
		tier.Id = bson.NewObjectId()
	}
	_, err := session.Database(defaultDBName).Collection(defaultTierCollection).InsertOne(ctx, tier)
	if err != nil {
		return nil, err
	}
	return tier, nil

}

func (repo *mongoRepository) DeleteTier(ctx context.Context, id string) error {
	log.Debug("received request to delete tier from db")

	session := repo.session
	m := bson.M{"_id": bson.ObjectIdHex(id)}

	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return err
	}

	_, err = session.Database(defaultDBName).Collection(defaultTierCollection).DeleteOne(ctx, m)
	return err
}

func (repo *mongoRepository) UpdateTier(ctx context.Context, tier *model.Tier) (*model.Tier, error) {
	log.Debug("received request to update tier")
	session := repo.session
	m := bson.M{"_id": tier.Id}
	upd := bson.M{"$set": bson.M{"backends": tier.Backends, "tenants": tier.Tenants}}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return nil, err
	}
	_, err = session.Database(defaultDBName).Collection(defaultTierCollection).UpdateOne(ctx, m, upd)
	if err != nil {
		return nil, err
	}
	return tier, nil
}

func (repo *mongoRepository) ListTiers(ctx context.Context, limit, offset int, query interface{}) ([]*model.Tier, error) {
	log.Debug("received request to list tiers")
	session := repo.session
	if limit == 0 {
		limit = math.MinInt32
	}
	var tiers []*model.Tier
	m := bson.M{}
	UpdateFilter(m, query.(map[string]string))
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return nil, err
	}
	log.Infof("ListTiers, limit=%d, offset=%d, m=%+v\n", limit, offset, m)

	cur, err := session.Database(defaultDBName).Collection(defaultTierCollection).Find(ctx, m)

	if err != nil {
		return nil, err
	}

	//Map result to slice
	for cur.Next(context.TODO()) {
		t := &model.Tier{}
		err := cur.Decode(&t)
		if err != nil {
			return tiers, err
		}
		tiers = append(tiers, t)
	}
	if err != nil {
		return nil, err
	}
	return tiers, nil
}

func (repo *mongoRepository) GetTier(ctx context.Context, id string) (*model.Tier,
	error) {
	log.Debug("received request to get tier details")
	session := repo.session
	m := bson.M{"_id": bson.ObjectIdHex(id)}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return nil, err
	}
	var tier = &model.Tier{}
	err = session.Database(defaultDBName).Collection(defaultTierCollection).FindOne(ctx, m).Decode(&tier)

	if err != nil {
		return nil, err
	}
	return tier, nil
}

func (repo *mongoRepository) Close() {
	repo.session.Disconnect(context.TODO())
}
