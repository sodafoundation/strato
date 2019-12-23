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
package yig

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/opensds/multi-cloud/backend/pkg/utils/constants"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/driver"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/config"
	_ "github.com/opensds/multi-cloud/s3/pkg/datastore/yig/meta/db"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/storage"
	log "github.com/sirupsen/logrus"
)

type YigDriverFactory struct {
	Drivers    sync.Map
	cfgWatcher *config.ConfigWatcher
	initLock   sync.Mutex
	initFlag   int32
}

func (ydf *YigDriverFactory) CreateDriver(backend *backendpb.BackendDetail) (driver.StorageDriver, error) {
	err := ydf.Init()
	if err != nil {
		log.Errorf("failed to perform YigDriverFactory init, err: %v", err)
		return nil, err
	}
	// if driver already exists, just return it.
	if driver, ok := ydf.Drivers.Load(backend.Endpoint); ok {
		return driver.(*storage.YigStorage), nil
	}

	log.Infof("no storage driver for yig endpoint %s", backend.Endpoint)
	return nil, errors.New(fmt.Sprintf("no storage driver for yig endpoint: %s", backend.Endpoint))
}

func (ydf *YigDriverFactory) Init() error {
	// check
	if atomic.LoadInt32(&ydf.initFlag) == 1 {
		return nil
	}

	// lock
	ydf.initLock.Lock()
	defer ydf.initLock.Unlock()

	// check
	if ydf.initFlag == 1 {
		return nil
	}

	// create the driver.
	rand.Seed(time.Now().UnixNano())

	// read the config.
	err := config.ReadConfigs("/etc/yig", ydf.driverInit)
	if err != nil {
		log.Errorf("failed to read yig configs, err: %v", err)
		return nil
	}

	// init config watcher.
	watcher, err := config.NewConfigWatcher(ydf.driverInit)
	if err != nil {
		log.Errorf("failed to new config watcher, err: %v", err)
		return err
	}
	ydf.cfgWatcher = watcher
	ydf.cfgWatcher.Watch("/etc/yig")

	atomic.StoreInt32(&ydf.initFlag, 1)
	return nil
}

func (ydf *YigDriverFactory) Close() {
	var keys []interface{}
	// stop config watcher
	if ydf.cfgWatcher != nil {
		ydf.cfgWatcher.Stop()
	}
	// close the drivers
	ydf.Drivers.Range(func(k, v interface{}) bool {
		drv := v.(*storage.YigStorage)
		drv.Close()
		keys = append(keys, k)
		return true
	})

	// remove the drivers
	for _, k := range keys {
		ydf.Drivers.Delete(k)
	}
}

func (ydf *YigDriverFactory) driverInit(cfg *config.Config) error {
	yigStorage, err := storage.New(cfg)
	if err != nil {
		log.Errorf("failed to create driver for %s, err: %v", cfg.Endpoint.Url, err)
		return err
	}

	ydf.Drivers.Store(cfg.Endpoint.Url, yigStorage)

	return nil
}

func init() {
	yigDf := &YigDriverFactory{}
	err := yigDf.Init()
	if err != nil {
		return
	}
	driver.AddCloser(yigDf)
	driver.RegisterDriverFactory(constants.BackendTypeYIGS3, yigDf)
}
