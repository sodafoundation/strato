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

package service

import (
	"context"
	"github.com/opensds/multi-cloud/metadata/pkg/db"
	resultpaginator "github.com/opensds/multi-cloud/metadata/pkg/result-paginator"
	"os"

	"github.com/micro/go-micro/v2/client"
	log "github.com/sirupsen/logrus"

	"github.com/aws/aws-sdk-go/aws/credentials"
	backend "github.com/opensds/multi-cloud/backend/proto"
	driver "github.com/opensds/multi-cloud/metadata/pkg/drivers/cloudfactory"
	querytranslator "github.com/opensds/multi-cloud/metadata/pkg/query-translator"
	metautils "github.com/opensds/multi-cloud/metadata/pkg/utils"
	validator "github.com/opensds/multi-cloud/metadata/pkg/validator"
	pb "github.com/opensds/multi-cloud/metadata/proto"
	"github.com/opensds/multi-cloud/s3/pkg/utils"
)

const (
	MICRO_ENVIRONMENT = "MICRO_ENVIRONMENT"
	K8S               = "k8s"

	metadataService_Docker = "metadata"
	metadataService_K8S    = "soda.multicloud.v1.metadata"
	backendService_Docker  = "backend"
	backendService_K8S     = "soda.multicloud.v1.backend"
)

type metadataService struct {
	metaClient    pb.MetadataService
	backendClient backend.BackendService
}

func NewMetaService() pb.MetadataHandler {

	log.Infof("Init metadata service finished.\n")

	metaService := metadataService_Docker

	if os.Getenv(MICRO_ENVIRONMENT) == K8S {
		metaService = metadataService_K8S
	}

	backendService := backendService_Docker

	if os.Getenv(MICRO_ENVIRONMENT) == K8S {
		backendService = backendService_K8S
	}

	return &metadataService{
		metaClient:    pb.NewMetadataService(metaService, client.DefaultClient),
		backendClient: backend.NewBackendService(backendService, client.DefaultClient),
	}
}

type S3Cred struct {
	Ak string
	Sk string
}

func (myc *S3Cred) IsExpired() bool {
	return false
}

func (myc *S3Cred) Retrieve() (credentials.Value, error) {
	cred := credentials.Value{AccessKeyID: myc.Ak, SecretAccessKey: myc.Sk}
	return cred, nil
}

func (f *metadataService) SyncMetadata(ctx context.Context, in *pb.SyncMetadataRequest, out *pb.BaseResponse) error {
	log.Infoln("Received sncMetadata request in metadata service.")
	// We need to find the backend in which bucket will be created
	backendName := "demo-aws"
	backend, err := utils.GetBackend(ctx, f.backendClient, backendName)
	if err != nil {
		log.Errorln("failed to get backend client with err:", err)
		return err
	}
	log.Debugln("the backend we got now....:%+v", backend)
	sd, err := driver.CreateStorageDriver(backend.Type, backend)
	if err != nil {
		log.Errorln("failed to create driver. err:", err)
		return err
	}

	err = sd.SyncMetadata(ctx, in)
	if err != nil {
		log.Errorln("failed to sync metadata in s3 service:", err)
		return err
	}

	return nil
}
func (f *metadataService) ListMetadata(ctx context.Context, in *pb.ListMetadataRequest, out *pb.ListMetadataResponse) error {
	log.Infof("received GetMetadata request in metadata service:%+v", in)

	log.Info(" validating list metadata resquest started.")

	//* validates the query options such as offset and limit and also the query
	okie, err := validator.ValidateInput(in)

	if !okie {
		log.Errorf("Failed to validate list metadata request: %v", err)
		return err
	}

	//* translates the query into database understood language
	translatedQuery := querytranslator.Translate(in)

	//* executes the translated query in the database and returns the result
	unPaginatedResult, err := db.DbAdapter.ListMetadata(ctx, translatedQuery)

	if err != nil {
		log.Errorf("Failed to execute query in database: %v", err)
		return err
	}

	log.Debugln("the un-paginated result is:", unPaginatedResult)

	paginatedResult := resultpaginator.Paginate(unPaginatedResult, in.GetLimit(), in.GetOffset())

	backends := metautils.GetBackends(paginatedResult)
	out.Backends = backends
	log.Info("resultant backends for user's query:", backends)

	return nil
}
