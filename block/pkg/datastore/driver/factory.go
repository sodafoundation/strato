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

package driver

import (
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	excep "github.com/opensds/multi-cloud/block/pkg/exception"
	log "github.com/sirupsen/logrus"
)

type DriverFactory interface {
	CreateDriver(detail *backendpb.BackendDetail) (StorageDriver, error)
}

var driverFactoryMgr = make(map[string]DriverFactory)

func RegisterDriverFactory(driverType string, factory DriverFactory) {
	log.Infof("Registering driver factory for driver type [%s]", driverType)
	driverFactoryMgr[driverType] = factory
}

func CreateStorageDriver(driverType string, detail *backendpb.BackendDetail) (StorageDriver, error) {
	log.Infof("Creating storage driver factory with driver type [%s]", driverType)
	if factory, ok := driverFactoryMgr[driverType]; ok {
		return factory.CreateDriver(detail)
	}
	return nil, excep.NoSuchType.Error()
}
