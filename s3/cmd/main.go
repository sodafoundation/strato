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

	"github.com/micro/go-micro"
	"github.com/opensds/multi-cloud/api/pkg/utils/obs"
	handler "github.com/opensds/multi-cloud/s3/pkg"
	_ "github.com/opensds/multi-cloud/s3/pkg/datastore"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/driver"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/config"
	"github.com/opensds/multi-cloud/s3/pkg/helper"
	"github.com/opensds/multi-cloud/s3/pkg/log"
	"github.com/opensds/multi-cloud/s3/pkg/meta/redis"
	pb "github.com/opensds/multi-cloud/s3/proto"
)

var logger *log.Logger

func main() {
	service := micro.NewService(
		micro.Name("s3"),
	)

	service.Init(micro.AfterStop(func() error {
		driver.FreeCloser()
		return nil
	}))

	helper.SetupConfig()

	//yig log
	f, err := os.OpenFile(helper.CONFIG.LogPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic("Failed to open log file " + helper.CONFIG.LogPath)
	}
	defer f.Close()

	logger = log.New(f, "[yig]", log.LstdFlags, helper.CONFIG.LogLevel)
	helper.Logger = logger
	logger.Printf(20, "YIG conf: %+v \n", helper.CONFIG)
	logger.Println(5, "YIG instance ID:", helper.CONFIG.InstanceId)

	//access log
	a, err := os.OpenFile(helper.CONFIG.AccessLogPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		panic("Failed to open access log file " + helper.CONFIG.AccessLogPath)
	}
	defer a.Close()
	accessLogger := log.New(a, "", 0, helper.CONFIG.LogLevel)
	helper.AccessLogger = accessLogger
	if helper.CONFIG.MetaCacheType > 0 || helper.CONFIG.EnableDataCache {
		//redis.Initialize()
		// read common config settings
		cc, err := config.ReadCommonConfig("/etc/yig")
		if err != nil {
			return
		}
		redis.Initialize(cc)
	}

	pb.RegisterS3Handler(service.Server(), handler.NewS3Service())
	if err := service.Run(); err != nil {
		fmt.Println(err)
	}
}
