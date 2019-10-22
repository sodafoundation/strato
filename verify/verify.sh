#!/bin/bash

# Copyright 2019 The OpenSDS Authors.
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

if ! [ -f "./bin/golangci-lint" ]; then echo "Download The files";
	curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh| sh -s v1.21.0
fi
export PATH=${PATH}:${GOPATH}/bin:${GOPATH}/src/github.com/opensds/multicloud/verify/bin
./bin/golangci-lint run --disable-all --enable=golint --enable=misspell --enable=gofmt ../api/... > golintoutput.txt
./bin/golangci-lint run --disable-all --enable=golint --enable=misspell --enable=gofmt ../backend/... >> golintoutput.txt
./bin/golangci-lint run --disable-all --enable=golint --enable=misspell --enable=gofmt ../dataflow/... >> golintoutput.txt
./bin/golangci-lint run --disable-all --enable=golint --enable=misspell --enable=gofmt ../datamover/... >> golintoutput.txt
./bin/golangci-lint run --disable-all --enable=golint --enable=misspell --enable=gofmt ../s3/... >> golintoutput.txt

if [ $? != 0 ]; then
       	echo "Please fix the warnings!"		       	
else
	echo "Gofmt,misspell,golint checks have been passed"
fi	

