// Copyright 2022 The SODA Authors.
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

	"github.com/opensds/multi-cloud/metadata/pkg/db"
	"github.com/opensds/multi-cloud/metadata/pkg/utils/config"

	log "github.com/sirupsen/logrus"

	handler "github.com/opensds/multi-cloud/metadata/pkg/service"
	pb "github.com/opensds/multi-cloud/metadata/proto"
)

const (
	MICRO_ENVIRONMENT = "MICRO_ENVIRONMENT"
	K8S               = "k8s"

	metadataService_Docker = "metadata"
	metadataService_K8S    = "soda.multicloud.v1.metadata"
)

func main() {
	dbHost := os.Getenv("DB_HOST")
	dbStore := &config.Database{Credential: "unkonwn", Driver: "mongodb", Endpoint: dbHost}
	db.Init(dbStore)
	defer db.Exit(dbStore)

	metadataService := metadataService_Docker

	if os.Getenv(MICRO_ENVIRONMENT) == K8S {
		metadataService = metadataService_K8S
	}

	service := micro.NewService(
		micro.Name(metadataService),
	)

	// obs.InitLogs()
	service.Init()

	log.Infof("Initializing metadata service..")

	pb.RegisterMetadataHandler(service.Server(), handler.NewMetaService())
	if err := service.Run(); err != nil {
		log.Error(err)
	}
	log.Infof("Init metadata service finished.\n")
}
