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
	"os"

	"github.com/emicklei/go-restful"
	"github.com/micro/go-micro/v2/web"
	"github.com/opensds/multi-cloud/api/pkg/backend"
	"github.com/opensds/multi-cloud/api/pkg/dataflow"
	"github.com/opensds/multi-cloud/api/pkg/file"
	"github.com/opensds/multi-cloud/api/pkg/filters/auth"
	"github.com/opensds/multi-cloud/api/pkg/filters/context"
	"github.com/opensds/multi-cloud/api/pkg/filters/logging"
	"github.com/opensds/multi-cloud/api/pkg/filters/signature/signer"
	"github.com/opensds/multi-cloud/api/pkg/s3"
	"github.com/opensds/multi-cloud/api/pkg/utils/obs"
	log "github.com/sirupsen/logrus"
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

	obs.InitLogs()
	wc := restful.NewContainer()

	flag := os.Getenv("SVC_FLAG")
	if flag == "s3" {
		s3ws := new(restful.WebService)
		s3ws.Path("/")
		s3ws.Doc("OpenSDS Multi-Cloud API")
		s3ws.Produces(restful.MIME_XML)

		s3ws.Filter(logging.FilterFactory())
		s3ws.Filter(context.FilterFactory())
		s3ws.Filter(signer.FilterFactory())
		s3.RegisterRouter(s3ws)

		wc.Add(s3ws)
	} else {
		ws := new(restful.WebService)
		ws.Path("/v1")
		ws.Doc("OpenSDS Multi-Cloud API")
		ws.Consumes(restful.MIME_JSON)
		ws.Produces(restful.MIME_JSON)

		backend.RegisterRouter(ws)
		dataflow.RegisterRouter(ws)
		file.RegisterRouter(ws)
		// add filter for authentication context
		ws.Filter(logging.FilterFactory())
		ws.Filter(context.FilterFactory())
		ws.Filter(auth.FilterFactory())
		wc.Add(ws)
	}

	webService.Handle("/", wc)
	if err := webService.Run(); err != nil {
		log.Fatal(err)
	}

}
