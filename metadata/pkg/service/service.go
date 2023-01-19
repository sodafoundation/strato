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
	"os"

	"github.com/micro/go-micro/v2/client"
	log "github.com/sirupsen/logrus"

	"github.com/aws/aws-sdk-go/aws/credentials"
	backend "github.com/opensds/multi-cloud/backend/proto"
	// "github.com/opensds/multi-cloud/metadata/pkg/db"
	driver "github.com/opensds/multi-cloud/metadata/pkg/drivers/cloudfactory"
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
	log.Infoln("the backend we got now....:%+v", backend)
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
	// log.Info("Received GetMetadata request in metadata service.")
	// res, err := db.DbAdapter.ListMetadata(ctx, in.Limit)
	// if err != nil {
	// 	log.Errorf("Failed to create backend: %v", err)
	// 	return err
	// }

	// var buckets []*pb.BucketMetadata
	// for _, buck := range res {
	// 	log.Infoln("buck.........", buck)
	// 	bucket := &pb.BucketMetadata{
	// 		Name:         buck.Name,
	// 		Type:         buck.BucketType,
	// 		Tags:         buck.BucketTags,
	// 		Region:       buck.Region,
	// 		CreationDate: buck.CreationDate.String(),
	// 	}
	// 	buckets = append(buckets, bucket)
	// }
	// log.Info("bucket:", buckets)

	// out.Buckets = buckets
	return nil
}
