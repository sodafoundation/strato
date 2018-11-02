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
DIST_DIR := $(BASE_DIR)/build/dist
VERSION ?= $(shell git describe --exact-match 2> /dev/null || \
	     git describe --match=$(git rev-parse --short=8 HEAD) \
             --always --dirty --abbrev=8)
BUILD_TGT := opensds-multicloud-$(VERSION)-linux-amd64

.PHONY: all build prebuild api backend s3 dataflow docker clean

all: build

build: api backend s3 dataflow datamover

prebuild:
	mkdir -p  $(BUILD_DIR)

api: prebuild
	CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o $(BUILD_DIR)/api github.com/opensds/multi-cloud/api/cmd

backend: prebuild
	CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o $(BUILD_DIR)/backend github.com/opensds/multi-cloud/backend/cmd

s3: prebuild
	CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o $(BUILD_DIR)/s3 github.com/opensds/multi-cloud/s3/cmd

dataflow: prebuild
	CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o $(BUILD_DIR)/dataflow github.com/opensds/multi-cloud/dataflow/cmd

datamover: prebuild
	CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-extldflags "-static"' -o $(BUILD_DIR)/datamover github.com/opensds/multi-cloud/datamover/cmd

docker: build

	cp $(BUILD_DIR)/api api
	chmod 755 api/api
	docker build api -t opensdsio/multi-cloud-api:latest

	cp $(BUILD_DIR)/backend backend
	chmod 755 backend/backend
	docker build backend -t opensdsio/multi-cloud-backend:latest

	cp $(BUILD_DIR)/s3 s3
	chmod 755 s3/s3
	docker build s3 -t opensdsio/multi-cloud-s3:latest

	cp $(BUILD_DIR)/dataflow dataflow
	chmod 755 dataflow/dataflow
	docker build dataflow -t opensdsio/multi-cloud-dataflow:latest

	cp $(BUILD_DIR)/datamover datamover
	chmod 755 datamover/datamover
	docker build datamover -t opensdsio/multi-cloud-datamover:latest

clean:
	rm -rf $(BUILD_DIR)

version:
	@echo ${VERSION}

.PHONY: dist
dist: build
	rm -fr $(DIST_DIR) && mkdir -p $(DIST_DIR)/$(BUILD_TGT)/bin
	cd $(DIST_DIR) && \
	cp ../api $(BUILD_TGT)/bin/ && \
	cp ../backend $(BUILD_TGT)/bin/ && \
	cp ../s3 $(BUILD_TGT)/bin/ && \
	cp ../dataflow $(BUILD_TGT)/bin/ && \
	cp ../datamover $(BUILD_TGT)/bin/ && \
	cp $(BASE_DIR)/LICENSE $(BUILD_TGT) && \
	zip -r $(DIST_DIR)/$(BUILD_TGT).zip $(BUILD_TGT) && \
	tar zcvf $(DIST_DIR)/$(BUILD_TGT).tar.gz $(BUILD_TGT)
	tree $(DIST_DIR)
