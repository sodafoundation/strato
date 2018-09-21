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

package main

import (
	"github.com/emicklei/go-restful"
	"github.com/micro/go-web"
	"github.com/opensds/multi-cloud/api/pkg/backend"
	"github.com/opensds/multi-cloud/api/pkg/dataflow"
	"github.com/opensds/multi-cloud/api/pkg/s3"

	"github.com/micro/go-log"
	//	_ "github.com/micro/go-plugins/client/grpc"
)

const (
	serviceName = "gelato"
)

func main() {
	webService := web.NewService(
		web.Name(serviceName),
		web.Version("v0.1.0"),
	)
	webService.Init()

	wc := restful.NewContainer()
	ws := new(restful.WebService)
	ws.Path("/v1")
	ws.Doc("OpenSDS Multi-Cloud API")
	ws.Consumes(restful.MIME_JSON)
	ws.Produces(restful.MIME_JSON)

	backend.RegisterRouter(ws)
	s3.RegisterRouter(ws)
	dataflow.RegisterRouter(ws)

	wc.Add(ws)
	webService.Handle("/", wc)
	if err := webService.Run(); err != nil {
		log.Fatal(err)
	}

}
