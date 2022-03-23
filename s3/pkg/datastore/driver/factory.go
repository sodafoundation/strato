package driver

import (
	backendpb "github.com/soda/multi-cloud/backend/proto"
	exp "github.com/soda/multi-cloud/s3/pkg/exception"
)

type DriverFactory interface {
	CreateDriver(detail *backendpb.BackendDetail) (StorageDriver, error)
}

var driverFactoryMgr = make(map[string]DriverFactory)

func RegisterDriverFactory(driverType string, factory DriverFactory) {
	driverFactoryMgr[driverType] = factory
}

func CreateStorageDriver(driverType string, detail *backendpb.BackendDetail) (StorageDriver, error) {
	if factory, ok := driverFactoryMgr[driverType]; ok {
		return factory.CreateDriver(detail)
	}
	return nil, exp.NoSuchType.Error()
}
