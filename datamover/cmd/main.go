// Copyright 2019 The OpenSDS Authors.
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

package main

import (
	"fmt"
	"os"
	"github.com/micro/go-micro/v2"
	"github.com/opensds/multi-cloud/api/pkg/utils/obs"
	datamover "github.com/opensds/multi-cloud/datamover/pkg"
	log "github.com/sirupsen/logrus"
)

func main() {

	datamoverService:= "datamover"	

	if(os.Getenv("MICRO_ENVIRONMENT") == "k8s"){
		datamoverService = "soda.multicloud.v1.dataflow"
	}

	service := micro.NewService(
		micro.Name(datamoverService),
	)

	obs.InitLogs()
	log.Info("Init datamover serivice")
	service.Init()

	datamover.InitDatamoverService()
	//pb.RegisterDatamoverHandler(service.Server(), handler.NewDatamoverService())
	if err := service.Run(); err != nil {
		fmt.Println(err)
	}
}
