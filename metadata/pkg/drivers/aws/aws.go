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

package aws

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	pb "github.com/opensds/multi-cloud/metadata/proto"
	log "github.com/sirupsen/logrus"
)

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

type AwsAdapter struct {
	Backend *backendpb.BackendDetail
	Session *session.Session
}

func (ad *AwsAdapter) SyncMetadata(ctx context.Context, in *pb.SyncMetadataRequest) error {
	log.Infoln("Implement me")
	return nil
}
func (ad *AwsAdapter) DownloadObject() {
	log.Info("Implement me")
}
