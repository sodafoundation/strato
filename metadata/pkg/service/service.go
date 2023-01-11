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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/globalsign/mgo/bson"
	"github.com/micro/go-micro/v2/client"
	log "github.com/sirupsen/logrus"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/opensds/multi-cloud/metadata/pkg/db"
	"github.com/opensds/multi-cloud/metadata/pkg/model"
	validator "github.com/opensds/multi-cloud/metadata/pkg/validator"
	pb "github.com/opensds/multi-cloud/metadata/proto"
)

const (
	MICRO_ENVIRONMENT = "MICRO_ENVIRONMENT"
	K8S               = "k8s"

	metadataService_Docker = "metadata"
	metadataService_K8S    = "soda.multicloud.v1.metadata"
)

type metadataService struct {
	metaClient pb.MetadataService
}

func NewMetaService() pb.MetadataHandler {

	log.Infof("Init metadata service finished.\n")

	metaService := metadataService_Docker

	if os.Getenv(MICRO_ENVIRONMENT) == K8S {
		metaService = metadataService_K8S
	}

	return &metadataService{
		metaClient: pb.NewMetadataService(metaService, client.DefaultClient),
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
	log.Info("Received sncMetadata request in metadata service.")
	//svc := s3.New(session.New())
	endpoint := "s3.ap-south-1.amazonaws.com"
	AccessKeyID := "AKIA6NODD4LSA36CQNVB"
	AccessKeySecret := "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"

	s3aksk := S3Cred{Ak: AccessKeyID, Sk: AccessKeySecret}
	creds := credentials.NewCredentials(&s3aksk)

	disableSSL := true
	sess, err := session.NewSession(&aws.Config{
		//Region:      &region,
		Endpoint:    &endpoint,
		Credentials: creds,
		DisableSSL:  &disableSSL,
	})
	if err != nil {
		return err
	}

	svc := s3.New(sess, aws.NewConfig().WithRegion("ap-south-1"))
	log.Info("svc done..")
	input := &s3.ListBucketsInput{}
	log.Info("input done..")

	result, err := svc.ListBuckets(input)
	log.Info("result done..")
	if err != nil {
		log.Infof("error is not nil..%s", err)
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				log.Infoln(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			log.Infoln("else err:", err.Error())
		}
		return nil
	}

	lres, err := db.DbAdapter.ListMetadata(ctx, 1000)
	if err != nil {
		log.Errorf("Failed to create backend: %v", err)
		return err
	}
	buckName := map[string]string{}
	for _, res := range lres {
		if _, ok := buckName[*res.Name]; !ok {
			buckName[*res.Name] = ""
		}
	}

	log.Infoln("result...:", result)

	var buckets []*model.MetaBucket
	for _, buck := range result.Buckets {
		if _, ok := buckName[*buck.Name]; !ok {
			bucket := &model.MetaBucket{
				Name:         buck.Name,
				Id:           bson.NewObjectId(),
				CreationDate: buck.CreationDate,
				Type:         "aws",
				Region:       endpoint,
			}
			buckets = append(buckets, bucket)
		}
	}
	log.Infoln("buckets...:", buckets)
	log.Infoln("bucketName...:%+v", buckName)

	err = db.DbAdapter.CreateMetadata(ctx, buckets)
	if err != nil {
		log.Errorf("Failed to create backend: %v", err)
		return err
	}

	log.Info("Got metadata successfully.")
	return nil
}

func (f *metadataService) ListMetadata(ctx context.Context, in *pb.ListMetadataRequest, out *pb.ListMetadataResponse) error {
	log.Info("received GetMetadata request in metadata service.")

	log.Info(" validating list metadata resquest started.")

	// validates the query options such as offset and limit and also the query
	okie, err := validator.ValidateInput(in)

	if !okie {
		log.Errorf("Failed to validate list metadata request: %v", err)
		return err
	}

	res, err := db.DbAdapter.ListMetadata(ctx, in.Limit)
	if err != nil {
		log.Errorf("Failed to create backend: %v", err)
		return err
	}

	var buckets []*pb.BucketMetadata
	for _, buck := range res {
		log.Infoln("buck.........", buck)
		bucket := &pb.BucketMetadata{
			Name:         *buck.Name,
			Type:         buck.Type,
			Tags:         buck.Tags,
			Region:       buck.Region,
			CreationDate: buck.CreationDate.String(),
		}
		buckets = append(buckets, bucket)
	}
	log.Info("bucket:", buckets)

	out.Buckets = buckets
	return nil
}
