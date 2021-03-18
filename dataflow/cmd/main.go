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
	"os"
	"github.com/micro/go-micro/v2"
	"github.com/opensds/multi-cloud/api/pkg/utils/obs"
	handler "github.com/opensds/multi-cloud/dataflow/pkg"
	"github.com/opensds/multi-cloud/dataflow/pkg/scheduler"
	_ "github.com/opensds/multi-cloud/dataflow/pkg/scheduler/trigger/crontrigger"
	pb "github.com/opensds/multi-cloud/dataflow/proto"
	log "github.com/sirupsen/logrus"
)

func main() {
	dataflowService := "dataflow"

	if(os.Getenv("MICRO_ENVIRONMENT") == "k8s"){
		dataflowService = "soda.multicloud.v1.dataflow"
	}

	service := micro.NewService(
		micro.Name(dataflowService),
	)

	obs.InitLogs()
	log.Info("Init dataflow service.")
	service.Init()
	pb.RegisterDataFlowHandler(service.Server(), handler.NewDataFlowService())
	scheduler.LoadAllPlans()
	scheduler.LoadLifecycleScheduler()
	if err := service.Run(); err != nil {
		log.Info(err)
	}
}
