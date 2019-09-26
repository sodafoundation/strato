package yig

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/opensds/multi-cloud/backend/pkg/utils/constants"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/driver"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/config"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/storage"
	"github.com/opensds/multi-cloud/s3/pkg/meta/redis"
	log "github.com/sirupsen/logrus"
)

type YigDriverFactory struct {
	Drivers    sync.Map
	cfgWatcher *config.ConfigWatcher
}

func (ydf *YigDriverFactory) CreateDriver(backend *backendpb.BackendDetail) (driver.StorageDriver, error) {
	// if driver already exists, just return it.
	if driver, ok := ydf.Drivers.Load(backend.Endpoint); ok {
		return driver.(*storage.YigStorage), nil
	}

	log.Infof("no storage driver for yig endpoint %s", backend.Endpoint)
	return nil, errors.New(fmt.Sprintf("no storage driver for yig endpoint: %s", backend.Endpoint))
}

func (ydf *YigDriverFactory) Init() error {
	// read common config settings
	cc, err := config.ReadCommonConfig("/etc/yig")
	if err != nil {
		return err
	}

	// create the driver.
	rand.Seed(time.Now().UnixNano())
	redis.Initialize(cc)

	// init config watcher.
	watcher, err := config.NewConfigWatcher(ydf.driverInit)
	if err != nil {
		log.Errorf("failed to new config watcher, err: %v", err)
		return err
	}
	ydf.cfgWatcher = watcher

	// read the config.
	err = config.ReadConfigs("/etc/yig", ydf.driverInit)
	if err != nil {
		log.Errorf("failed to read yig configs, err: %v", err)
		return err
	}

	ydf.cfgWatcher.Watch("/etc/yig")
	return nil
}

func (ydf *YigDriverFactory) Close() {
	var keys []interface{}
	// stop config watcher
	ydf.cfgWatcher.Stop()
	// close the drivers
	ydf.Drivers.Range(func(k, v interface{}) bool {
		drv := v.(*storage.YigStorage)
		drv.DriverClose()
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
