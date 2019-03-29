// Copyright (c) 2018 Huawei Technologies Co., Ltd. All Rights Reserved.
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

package pkg

import (
	"github.com/go-log/log"
	"github.com/opensds/multi-cloud/dataflow/pkg/utils"
	"github.com/opensds/multi-cloud/datamover/pkg/db"
	"github.com/opensds/multi-cloud/datamover/pkg/kafka"
	"os"
	"strings"
)

var dataMoverGroup = "datamover"

func InitDatamoverService() error {
	host := os.Getenv("DB_HOST")
	dbstor := utils.Database{Credential: "unkonwn", Driver: "mongodb", Endpoint: host}
	db.Init(&dbstor)

	addrs := []string{}
	config := strings.Split(os.Getenv("KAFKA_ADVERTISED_LISTENERS"), ";")
	for i := 0; i < len(config); i++ {
		addr := strings.Split(config[i], "//")
		if len(addr) != 2 {
			log.Log("Invalid addr:", config[i])
		} else {
			addrs = append(addrs, addr[1])
		}
	}
	topics := []string{"migration"}
	err := kafka.Init(addrs, dataMoverGroup, topics)
	if err != nil {
		log.Log("Init kafka consumer failed.")
		return nil
	}
	go kafka.LoopConsume()

	datamoverID := os.Getenv("HOSTNAME")
	log.Logf("Init datamover[ID#%s] finished.\n", datamoverID)
	return nil
}
