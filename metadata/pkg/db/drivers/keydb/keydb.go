package keydb

// Copyright 2023 The OpenSDS Authors.
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

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/go-redis/redis"
	"github.com/opensds/multi-cloud/metadata/pkg/constants"
	"github.com/opensds/multi-cloud/metadata/pkg/model"
	log "github.com/sirupsen/logrus"
	"sync"
)

var adap = &Adapter{}
var mutex sync.Mutex
var CACHE_HASH = "CACHE_HASH"

func InitCache(uri string) *Adapter {
	mutex.Lock()
	defer mutex.Unlock()

	if adap.session != nil {
		return adap
	}
	// Create a new client and connect to the server
	client := redis.NewClient(&redis.Options{
		Addr:     uri,
		Password: "",
		DB:       0,
	})

	_, err := client.Ping().Result()
	if err != nil {
		log.Errorf("Error connecting:%v", err.Error())
		panic(err)
	}
	log.Infoln("successfully connected and pinged from cache.")
	adap.session = client
	return adap
}

func ExitCache() {
	adap.session.Close()
}

type Adapter struct {
	session *redis.Client
}

func (a Adapter) StoreData(ctx context.Context, result interface{}, query interface{}) error {
	queryStr, err := json.Marshal(query)
	if err != nil {
		return err
	}

	resultStr, err := json.Marshal(result)
	if err != nil {
		return err
	}

	if len(resultStr) >= constants.CACHE_LIMIT {
		return errors.New("size of result is more than allowed limit for cache")
	}

	txPipeline := a.session.TxPipeline()
	txPipeline.Do(constants.HSET, CACHE_HASH, string(queryStr), string(resultStr))
	//* Setting the TTL
	txPipeline.Do(constants.EXPIREMEMBER, CACHE_HASH, string(queryStr), constants.CACHE_TTL_IN_SECONDS)
	_, err = txPipeline.Exec()
	return err
}

func (a Adapter) FetchData(ctx context.Context, query interface{}) ([]*model.MetaBackend, error) {
	queryStr, err := json.Marshal(query)
	var result []*model.MetaBackend
	if err != nil {
		return nil, err
	}
	cachedResult := a.session.HGet(CACHE_HASH, string(queryStr))
	cachedResultStr, err := cachedResult.Result()
	if err == redis.Nil {
		// redis returns redis.Nil error msg when its not present in cache
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	err = json.Unmarshal([]byte(cachedResultStr), &result)
	if err != nil {
		return nil, err
	}
	return result, nil
}
