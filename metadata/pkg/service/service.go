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
	driver "github.com/opensds/multi-cloud/metadata/pkg/drivers/cloudfactory"
	"github.com/opensds/multi-cloud/metadata/pkg/model"
	queryrunner "github.com/opensds/multi-cloud/metadata/pkg/query-runner"
	querytranslator "github.com/opensds/multi-cloud/metadata/pkg/query-translator"
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
	log.Info("received GetMetadata request in metadata service.")

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
	unPaginatedResult, err := queryrunner.ExecuteQuery(ctx, translatedQuery)

	if err != nil {
		log.Errorf("Failed to execute query in database: %v", err)
		return err
	}

	var backendMetadatas []*pb.BackendMetadata

	log.Info("unPaginatedResult......:", unPaginatedResult)

	out.Backends = backendMetadatas

	protoBackends := GetProtoBackends(unPaginatedResult)
	out.Backends = protoBackends
	log.Info("proto backends:", protoBackends)

	return nil
}

func GetProtoBackends(unPaginatedResult []*model.MetaBackend) []*pb.BackendMetadata {
	var protoBackends []*pb.BackendMetadata
	for _, backend := range unPaginatedResult {
		var protoBuckets []*pb.BucketMetadata
		protoBuckets = GetProtoBuckets(backend, protoBuckets)
		protoBackend := &pb.BackendMetadata{
			BackendName: backend.BackendName,
			Region:      backend.Region,
			Type:        backend.Type,
			Buckets:     protoBuckets,
		}
		protoBackends = append(protoBackends, protoBackend)
	}
	return protoBackends
}

func GetProtoBuckets(backend *model.MetaBackend, protoBuckets []*pb.BucketMetadata) []*pb.BucketMetadata {
	for _, bucket := range backend.Buckets {

		protoObjects := GetProtoObjects(bucket)

		protoBucket := &pb.BucketMetadata{
			Name:             bucket.Name,
			Type:             bucket.Type,
			Region:           bucket.Region,
			TotalSizeInBytes: bucket.TotalSize,
			NumberOfObjects:  int32(bucket.NumberOfObjects),
			CreationDate:     bucket.CreationDate.String(),
			Tags:             bucket.BucketTags,
			Objects:          protoObjects,
		}
		protoBuckets = append(protoBuckets, protoBucket)
	}
	return protoBuckets
}

func GetProtoObjects(bucket model.MetaBucket) []*pb.ObjectMetadata {
	var protoObjects []*pb.ObjectMetadata

	for _, object := range bucket.Objects {
		protoObject := &pb.ObjectMetadata{
			Name:                        object.ObjectName,
			LastModifiedDate:            object.LastModifiedDate.String(),
			SizeInBytes:                 int32(object.Size),
			BucketName:                  bucket.Name,
			Type:                        object.ObjectType,
			ServerSideEncryptionEnabled: object.ServerSideEncryptionEnabled,
			ExpiresDate:                 object.ExpiresDate.String(),
			GrantControl:                object.GrantControl,
			VersionId:                   string(object.VersionId),
			RedirectLocation:            object.RedirectLocation,
			ReplicationStatus:           object.ReplicationStatus,
			Tags:                        object.ObjectTags,
			Metadata:                    object.Metadata,
		}
		protoObjects = append(protoObjects, protoObject)
	}
	return protoObjects
}
