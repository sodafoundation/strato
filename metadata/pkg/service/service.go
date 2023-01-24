// Copyright 2023 The SODA Authors.
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
	"github.com/opensds/multi-cloud/metadata/pkg/model"
	"go.mongodb.org/mongo-driver/bson"
	"os"

	"github.com/opensds/multi-cloud/metadata/pkg/db"
	querymanager "github.com/opensds/multi-cloud/metadata/pkg/query-manager"

	"github.com/micro/go-micro/v2/client"
	log "github.com/sirupsen/logrus"

	"github.com/aws/aws-sdk-go/aws/credentials"
	backend "github.com/opensds/multi-cloud/backend/proto"
	driver "github.com/opensds/multi-cloud/metadata/pkg/drivers/cloudfactory"
	utils "github.com/opensds/multi-cloud/metadata/pkg/utils"
	pb "github.com/opensds/multi-cloud/metadata/proto"
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

func Sync(ctx context.Context, backend *backend.BackendDetail, in *pb.SyncMetadataRequest) error {
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

func (f *metadataService) SyncMetadata(ctx context.Context, in *pb.SyncMetadataRequest, out *pb.BaseResponse) error {
	log.Infoln("received sncMetadata request in metadata service:%+v", in)
	if in.Id != "" {
		backend, err := utils.GetBackend(ctx, f.backendClient, in.Id)
		if err != nil {
			log.Errorln("failed to get backend client with err:", err)
			return err
		}
		Sync(ctx, backend, in)
	} else {
		backends, err := utils.ListBackend(ctx, f.backendClient)
		if err != nil {
			log.Errorln("failed to get backend client with err:", err)
			return err
		}
		for _, backend := range backends {
			go Sync(ctx, backend, in)
		}
	}

	return nil
}

func (f *metadataService) ListMetadata(ctx context.Context, in *pb.ListMetadataRequest, out *pb.ListMetadataResponse) error {
	log.Infof("received GetMetadata request in metadata service:%+v", in)

	//* validates the query options such as offset and limit and also the query
	okie, err := querymanager.ValidateInput(in)

	if !okie {
		log.Errorf("Failed to validate list metadata request: %v", err)
		return err
	}

	//* translates the query into database understood language
	translatedQuery := querymanager.Translate(in)

	//* Fetches the result if present in cache
	result := fetchQueryResultFromCache(ctx, err, translatedQuery)
	if result == nil {
		//* executes the translated query in the database and returns the result
		unPaginatedResult, err := db.DbAdapter.ListMetadata(ctx, translatedQuery)
		if err != nil {
			log.Errorf("Failed to execute query in database: %v", err)
			return err
		}

		//* store query result in cache
		storeQueryResultInCache(ctx, err, unPaginatedResult, translatedQuery)
		result = unPaginatedResult
	} else {
		log.Infoln("Result fetched from Cache")
	}

	paginatedResult := querymanager.Paginate(result, in.GetLimit(), in.GetOffset())
	backends := utils.GetBackends(paginatedResult)
	out.Backends = backends

	return nil
}

func fetchQueryResultFromCache(ctx context.Context, err error, translatedQuery []bson.D) []*model.MetaBackend {
	result, err := db.Cacheadapter.FetchData(ctx, translatedQuery)
	if err != nil {
		log.Errorf("Failed to fetch query result in cache: %v", err)
	}
	return result
}

func storeQueryResultInCache(ctx context.Context, err error, unPaginatedResult []*model.MetaBackend, translatedQuery []bson.D) {
	if err = db.Cacheadapter.StoreData(ctx, unPaginatedResult, translatedQuery); err != nil {
		log.Errorf("Failed to store query result in cache: %v", err)
	} else {
		log.Infoln("Result of query stored in cache")
	}
}
