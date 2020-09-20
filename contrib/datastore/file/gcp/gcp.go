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

// This file includes GCP Driver Implementation has taken reference from
// gcp-filestore-csi-driver which is a Google Cloud FileStore
// Container Storage Interface (CSI) Plugin.

// https://github.com/kubernetes-sigs/gcp-filestore-csi-driver

package gcp

import (
	"context"
	"fmt"

	"github.com/micro/go-micro/v2/util/log"
	"github.com/opensds/multi-cloud/contrib/utils"

	backendpb "github.com/opensds/multi-cloud/backend/proto"
	file "github.com/opensds/multi-cloud/file/proto"
	gcpfilev1 "google.golang.org/api/file/v1"
)

const (
	locationURIFmt  = "projects/%s/locations/%s"
)

type GcpAdapter struct {
	backend           *backendpb.BackendDetail
	fileService       *gcpfilev1.Service
	instancesService  *gcpfilev1.ProjectsLocationsInstancesService
	operationsService *gcpfilev1.ProjectsLocationsOperationsService
}

func (g GcpAdapter) CreateFileShare(ctx context.Context, fs *file.CreateFileShareRequest) (*file.CreateFileShareResponse, error) {
	instanceName := fs.Fileshare.Name + "-instance"
	labels := make(map[string]string)
	for _, tag := range fs.Fileshare.Tags {
		labels[tag.Key] = tag.Value
	}

	instance := &gcpfilev1.Instance{
		Description: fs.Fileshare.Description,
		Tier: fs.Fileshare.Metadata.Fields[Tier].GetStringValue(),
		FileShares: []*gcpfilev1.FileShareConfig{
			{
				Name:       fs.Fileshare.Name,
				CapacityGb: fs.Fileshare.Size / utils.GB_FACTOR,
			},
		},
		Networks: []*gcpfilev1.NetworkConfig{
			{
				Network:         DefaultNetwork,
				Modes:           []string{InternetProtocolModeIpv4},
			},
		},
		Labels: labels,
	}

	log.Infof("Starting CreateInstance on cloud backend")
	log.Debugf("Creating instance %s: location %s, tier %s, capacity %s, labels %v", instanceName,
		fs.Fileshare.AvailabilityZone, instance.Tier, instance.FileShares[0].CapacityGb, instance.Labels)

	op, err := g.instancesService.Create(g.locationURI(g.backend.Region,
		fs.Fileshare.AvailabilityZone), instance).InstanceId(instanceName).Context(ctx).Do()
	if err != nil {
		return nil, fmt.Errorf("createInstance operation failed: %v", err)
	}
	log.Infof("Create File share, Operation Resource: %s submitted to cloud backend", op.Name)

	return &file.CreateFileShareResponse{
		Fileshare: fs.Fileshare,
	}, nil
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

func (g GcpAdapter) locationURI(project, location string) string {
	return fmt.Sprintf(locationURIFmt, project, location)
}


func (g GcpAdapter) Close() error {
	panic("implement me")
}

