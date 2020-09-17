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

package main

import (
	"fmt"
	"os"

	"github.com/micro/go-micro/v2"
	"github.com/opensds/multi-cloud/api/pkg/utils/obs"
	"github.com/opensds/multi-cloud/block/pkg/db"
	"github.com/opensds/multi-cloud/block/pkg/utils/config"
	_ "github.com/opensds/multi-cloud/contrib/datastore"

	handler "github.com/opensds/multi-cloud/block/pkg/service"
	pb "github.com/opensds/multi-cloud/block/proto"
	log "github.com/sirupsen/logrus"
)

func main() {
	dbHost := os.Getenv("DB_HOST")
	dbStore := &config.Database{Credential: "unkonwn", Driver: "mongodb", Endpoint: dbHost}
	db.Init(dbStore)
	defer db.Exit(dbStore)

	service := micro.NewService(
		micro.Name("block"),
	)

	obs.InitLogs()
	service.Init()

	log.Infof("Initializing Block service..")

	pb.RegisterBlockHandler(service.Server(), handler.NewBlockService())
	if err := service.Run(); err != nil {
		fmt.Println(err)
	}
}
