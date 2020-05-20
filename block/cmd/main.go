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

package main

import (
	"fmt"

	"github.com/micro/go-micro"
	"github.com/opensds/multi-cloud/api/pkg/utils/obs"
	handler "github.com/opensds/multi-cloud/block/pkg/service"
	pb "github.com/opensds/multi-cloud/block/proto"
	log "github.com/sirupsen/logrus"
)

func main() {
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
