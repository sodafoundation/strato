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
	exp "github.com/opensds/multi-cloud/s3/pkg/exception"
)

type DriverFactory interface {
	CreateFileStorageDriver(backend *backendpb.BackendDetail) (FileStorageDriver, error)
}

var driverFactoryMgr = make(map[string]DriverFactory)

func RegisterDriverFactory(driverType string, factory DriverFactory) {
	driverFactoryMgr[driverType] = factory
}

func CreateStorageDriver(backend *backendpb.BackendDetail) (StorageDriver, error) {
	if factory, ok := driverFactoryMgr[backend.Type]; ok {
		return factory.CreateFileStorageDriver(backend)
	}
	return nil, exp.NoSuchType.Error()
}
