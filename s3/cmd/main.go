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

	"github.com/micro/go-micro/v2"
	"github.com/opensds/multi-cloud/api/pkg/utils/obs"
	_ "github.com/opensds/multi-cloud/s3/pkg/datastore"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/driver"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/config"
	gc "github.com/opensds/multi-cloud/s3/pkg/gc"
	"github.com/opensds/multi-cloud/s3/pkg/helper"
	"github.com/opensds/multi-cloud/s3/pkg/meta/redis"
	handler "github.com/opensds/multi-cloud/s3/pkg/service"
	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
	_ "go.uber.org/automaxprocs"
)

func main() {
	service := micro.NewService(
		micro.Name("s3"),
	)

	obs.InitLogs()
	service.Init(micro.AfterStop(func() error {
		driver.FreeCloser()
		gc.Stop()
		return nil
	}))

	helper.SetupConfig()

	log.Infof("YIG conf: %+v \n", helper.CONFIG)
	log.Infof("YIG instance ID:", helper.CONFIG.InstanceId)

	if helper.CONFIG.MetaCacheType > 0 || helper.CONFIG.EnableDataCache {
		cfg := config.CacheConfig{
			Mode:    helper.CONFIG.RedisMode,
			Address: helper.CONFIG.RedisAddress,
		}
		redis.Initialize(&cfg)
	}

	pb.RegisterS3Handler(service.Server(), handler.NewS3Service())
	if err := service.Run(); err != nil {
		fmt.Println(err)
	}
}
