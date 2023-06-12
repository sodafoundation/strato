package cloudfactory

import (
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	exp "github.com/opensds/multi-cloud/s3/pkg/exception"
	log "github.com/sirupsen/logrus"
)

type DriverFactory interface {
	CreateDriver(detail *backendpb.BackendDetail) (CloudDriver, error)
}

var driverFactoryMgr = make(map[string]DriverFactory)

func RegisterDriverFactory(driverType string, factory DriverFactory) {
	driverFactoryMgr[driverType] = factory
}

func CreateStorageDriver(driverType string, detail *backendpb.BackendDetail) (CloudDriver, error) {
	log.Infof("Creting driver of type:%s", driverType)

	if factory, ok := driverFactoryMgr[driverType]; ok {
		return factory.CreateDriver(detail)
	}
	return nil, exp.NoSuchType.Error()
}
