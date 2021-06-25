// Copyright 2021 The SODA Authors.
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
	log "github.com/sirupsen/logrus"

	"github.com/opensds/multi-cloud/aksk/pkg/iam"
	handler "github.com/opensds/multi-cloud/aksk/pkg/service"
	"github.com/opensds/multi-cloud/aksk/pkg/utils/config"
	pb "github.com/opensds/multi-cloud/aksk/proto"
	"github.com/opensds/multi-cloud/api/pkg/utils/obs"
)

func main() {
	iamHost := os.Getenv("IAM_HOST")
	iam.Init(&config.CredentialStore{
		URI:    "/identity/v3/credentials",
		Driver: "keystone",
		Host:   iamHost})

	obs.InitLogs()
	service := micro.NewService(
		micro.Name("aksk"),
	)
	service.Init()
	pb.RegisterAkSkHandler(service.Server(), handler.NewAkSkService())
	if err := service.Run(); err != nil {
		log.Error(err)
	}
}
