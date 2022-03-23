#!/bin/bash

# Copyright 2018 The soda Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Download and install mockery tool.
go get -v github.com/vektra/mockery/.../

# Auto-generate some fake objects in "file" module for mocking work.
mockery -name DBAdapter -dir ../file/pkg/db -output ./file/db/testing -case underscore

# Auto-generate some fake objects in "backend" module for mocking work.
mockery -name Repository -dir ../backend/pkg/db -output ./backend/db/testing -case underscore
mockery -name BackendService -dir ../backend/proto -output ./backend/db/proto -case underscore

# Auto-generate some fake objects in "contrib" module for mocking work.
mockery -name StorageDriver -dir ../contrib/datastore/drivers -output ./contrib/datastore/drivers -case underscore
mockery -name DriverFactory -dir ../contrib/datastore/drivers -output ./contrib/datastore/drivers -case underscore
mockery -name FileStorageDriver -dir ../contrib/datastore/drivers -output ./contrib/datastore/drivers -case underscore