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

package gcp

import (
	"context"

	backendpb "github.com/opensds/multi-cloud/backend/proto"
	pb "github.com/opensds/multi-cloud/metadata/proto"
	log "github.com/sirupsen/logrus"
	"github.com/webrtcn/s3client"
)

type GcpAdapter struct {
	backend *backendpb.BackendDetail
	session *s3client.Client
}

func (ad *GcpAdapter) SyncMetadata(ctx context.Context, in *pb.SyncMetadataRequest) error {
	log.Infoln("Implement me (gcp) driver")
	return nil
}
func (ad *GcpAdapter) DownloadObject() {
	log.Info("Implement me (gcp) driver")
}
