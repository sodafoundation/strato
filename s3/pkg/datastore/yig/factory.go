package yig

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/opensds/multi-cloud/backend/pkg/utils/constants"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/driver"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/config"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/storage"
	"github.com/opensds/multi-cloud/s3/pkg/helper"
	"github.com/opensds/multi-cloud/s3/pkg/log"
	"github.com/opensds/multi-cloud/s3/pkg/meta/redis"
)

type YigDriverFactory struct {
	Drivers    sync.Map
	cfgWatcher *config.ConfigWatcher
	// for common log file.
	logfile *os.File
}

func (ydf *YigDriverFactory) CreateDriver(backend *backendpb.BackendDetail) (driver.StorageDriver, error) {
	// if driver already exists, just return it.
	if driver, ok := ydf.Drivers.Load(backend.Endpoint); ok {
		return driver.(*storage.YigStorage), nil
	}

	helper.Logger.Printf(2, "no storage driver for yig endpoint %s", backend.Endpoint)
	return nil, errors.New(fmt.Sprintf("no storage driver for yig endpoint: %s", backend.Endpoint))
}

func (ydf *YigDriverFactory) Init() error {
	// read common config settings
	cc, err := config.ReadCommonConfig("/etc/yig")
	if err != nil {
		return err
	}
	// init common log file handler.
	filename := filepath.Join(cc.Log.Path, "common.log")
	logf, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	helper.Logger = log.New(logf, "[yig]", log.LstdFlags, cc.Log.Level)
	// create the driver.
	rand.Seed(time.Now().UnixNano())
	redis.Initialize(cc)

	// init config watcher.
	watcher, err := config.NewConfigWatcher(ydf.driverInit)
	if err != nil {
		helper.Logger.Printf(2, "failed to new config watcher, err: %v", err)
		return err
	}
	ydf.cfgWatcher = watcher

	// read the config.
	err = config.ReadConfigs("/etc/yig", ydf.driverInit)
	if err != nil {
		helper.Logger.Printf(2, "failed to read yig configs, err: %v", err)
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
		drv.Close()
		keys = append(keys, k)
		return true
	})

	// remove the drivers
	for _, k := range keys {
		ydf.Drivers.Delete(k)
	}

	ydf.logfile.Close()
}

func (ydf *YigDriverFactory) driverInit(cfg *config.Config) error {
	yigStorage, err := storage.New(cfg)
	if err != nil {
		helper.Logger.Printf(2, "failed to create driver for %s, err: %v", cfg.Endpoint.Url, err)
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
