// Copyright 2020 The OpenSDS Authors.
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

package service

import (
	"os"
	_ "strings"

	"github.com/micro/go-micro/v2/client"
	backend "github.com/opensds/multi-cloud/backend/proto"
	_ "github.com/opensds/multi-cloud/block/pkg/datastore/aws"
	pb "github.com/opensds/multi-cloud/block/proto"
	"github.com/opensds/multi-cloud/dataflow/pkg/utils"
	"github.com/opensds/multi-cloud/datamover/pkg/db"
	log "github.com/sirupsen/logrus"
)

type blockService struct {
	backendClient backend.BackendService
}

func NewBlockService() pb.BlockHandler {
	host := os.Getenv("DB_HOST")
	dbstor := utils.Database{Credential: "unkonwn", Driver: "mongodb", Endpoint: host}
	db.Init(&dbstor)

	log.Infof("Init block service finished.\n")
	return &blockService{
		backendClient: backend.NewBackendService("backend", client.DefaultClient),
	}
}
