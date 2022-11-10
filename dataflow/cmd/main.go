// Copyright 2019 The soda Authors.
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

	"github.com/soda/multi-cloud/api/pkg/utils/obs"
	handler "github.com/soda/multi-cloud/dataflow/pkg"
	"github.com/soda/multi-cloud/dataflow/pkg/scheduler"
	_ "github.com/soda/multi-cloud/dataflow/pkg/scheduler/trigger/crontrigger"
	dataflow "github.com/soda/multi-cloud/dataflow/proto"

	"github.com/micro/go-micro/v2"
	log "github.com/sirupsen/logrus"
)

const (
	MICRO_ENVIRONMENT = "MICRO_ENVIRONMENT"
	K8S               = "k8s"

	dataflowService_Docker = "dataflow"
	dataflowService_K8S    = "soda.multicloud.v1.dataflow"
)

func main() {
	dataflowService := dataflowService_Docker

	if os.Getenv(MICRO_ENVIRONMENT) == K8S {
		dataflowService = dataflowService_K8S
	}

	service := micro.NewService(
		micro.Name(dataflowService),
	)

	obs.InitLogs()
	log.Info("Init dataflow service.")
	service.Init()
	dataflow.RegisterDataFlowHandler(service.Server(), handler.NewDataFlowService())
	scheduler.LoadAllPlans()
	scheduler.LoadLifecycleScheduler()
	if err := service.Run(); err != nil {
		log.Info(err)
	}
}
