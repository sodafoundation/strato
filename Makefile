# Copyright (c) 2018 Huawei Technologies Co., Ltd. All Rights Reserved.
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

BASE_DIR := $(shell pwd)
BUILD_DIR := $(BASE_DIR)/build

.PHONY: all build prebuild api backend s3 dataflow docker clean

all: build

build: api backend s3 dataflow datamover

prebuild:
	mkdir -p  $(BUILD_DIR)

api: prebuild
	CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o $(BUILD_DIR)/api github.com/opensds/go-panda/api/cmd

backend: prebuild
	CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o $(BUILD_DIR)/backend github.com/opensds/go-panda/backend/cmd

s3: prebuild
	CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o $(BUILD_DIR)/s3 github.com/opensds/go-panda/s3/cmd

dataflow: prebuild
	CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o $(BUILD_DIR)/dataflow github.com/opensds/go-panda/dataflow/cmd

datamover: prebuild
	CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o $(BUILD_DIR)/datamover github.com/opensds/go-panda/datamover/cmd

docker: build
	cp $(BUILD_DIR)/api api
	docker build api -t opensdsio/go-panda/api:latest

	cp $(BUILD_DIR)/backend backend
	docker build backend -t opensdsio/go-panda/backend:latest

	cp $(BUILD_DIR)/s3 s3
	docker build s3 -t opensdsio/go-panda/s3:latest

	cp $(BUILD_DIR)/dataflow dataflow
	docker build dataflow -t opensdsio/go-panda/dataflow:latest

	cp $(BUILD_DIR)/datamover datamover
	docker build datamover -t opensdsio/go-panda/datamover:latest
clean:
	rm -rf $(BUILD_DIR) 
