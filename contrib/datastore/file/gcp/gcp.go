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

package gcp

import (
	"context"

	backendpb "github.com/opensds/multi-cloud/backend/proto"
	file "github.com/opensds/multi-cloud/file/proto"
	gcpfilev1 "google.golang.org/api/file/v1"
)

type GcpAdapter struct {
	backend           *backendpb.BackendDetail
	fileService       *gcpfilev1.Service
	instancesService  *gcpfilev1.ProjectsLocationsInstancesService
	operationsService *gcpfilev1.ProjectsLocationsOperationsService
}

func (g GcpAdapter) CreateFileShare(ctx context.Context, fs *file.CreateFileShareRequest) (*file.CreateFileShareResponse, error) {
	panic("implement me")
}

func (g GcpAdapter) GetFileShare(ctx context.Context, fs *file.GetFileShareRequest) (*file.GetFileShareResponse, error) {
	panic("implement me")
}

func (g GcpAdapter) ListFileShare(ctx context.Context, fs *file.ListFileShareRequest) (*file.ListFileShareResponse, error) {
	panic("implement me")
}

func (g GcpAdapter) UpdatefileShare(ctx context.Context, fs *file.UpdateFileShareRequest) (*file.UpdateFileShareResponse, error) {
	panic("implement me")
}

func (g GcpAdapter) DeleteFileShare(ctx context.Context, fs *file.DeleteFileShareRequest) (*file.DeleteFileShareResponse, error) {
	panic("implement me")
}

func (g GcpAdapter) Close() error {
	panic("implement me")
}

