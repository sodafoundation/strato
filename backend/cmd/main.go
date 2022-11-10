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
	"fmt"
	"os"

	micro "github.com/micro/go-micro/v2"

	"github.com/soda/multi-cloud/api/pkg/utils/obs"
	"github.com/soda/multi-cloud/backend/pkg/db"
	handler "github.com/soda/multi-cloud/backend/pkg/service"
	"github.com/soda/multi-cloud/backend/pkg/utils/config"
	pb "github.com/soda/multi-cloud/backend/proto"
)

const (
	MICRO_ENVIRONMENT = "MICRO_ENVIRONMENT"
	K8S               = "k8s"

	backendService_Docker = "backend"
	backendService_K8S    = "soda.multicloud.v1.backend"
)

func main() {
	dbHost := os.Getenv("DB_HOST")
	db.Init(&config.Database{
		Driver:   "mongodb",
		Endpoint: dbHost})
	defer db.Exit()

	obs.InitLogs()

	backendService := backendService_Docker

	if os.Getenv(MICRO_ENVIRONMENT) == K8S {
		backendService = backendService_K8S
	}

	service := micro.NewService(
		micro.Name(backendService),
	)
	service.Init()

	pb.RegisterBackendHandler(service.Server(), handler.NewBackendService())
	if err := service.Run(); err != nil {
		fmt.Println(err)
	}
}
