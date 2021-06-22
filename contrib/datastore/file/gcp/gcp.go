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
	"time"

	"github.com/micro/go-micro/v2/util/log"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/opensds/multi-cloud/contrib/utils"

	gcpfilev1 "google.golang.org/api/file/v1"

	backendpb "github.com/opensds/multi-cloud/backend/proto"
	file "github.com/opensds/multi-cloud/file/proto"
)

const (
	locationURIFmt = "projects/%s/locations/%s"
	instanceURIFmt = locationURIFmt + "/instances/%s"

	// GCP FS Patch update masks
	fileShareUpdateMask = "file_shares"
)

var createOp, updateOp, deleteOp *gcpfilev1.Operation

type GcpAdapter struct {
	backend           *backendpb.BackendDetail
	fileService       *gcpfilev1.Service
	instancesService  *gcpfilev1.ProjectsLocationsInstancesService
	operationsService *gcpfilev1.ProjectsLocationsOperationsService
}

func (g GcpAdapter) CreateFileShare(ctx context.Context, fs *file.CreateFileShareRequest) (*file.CreateFileShareResponse, error) {
	instanceId := g.getInstanceId(fs.Fileshare.Name)
	labels := make(map[string]string)
	for _, tag := range fs.Fileshare.Tags {
		labels[tag.Key] = tag.Value
	}

	instance := &gcpfilev1.Instance{
		Description: fs.Fileshare.Description,
		Tier:        fs.Fileshare.Metadata.Fields[Tier].GetStringValue(),
		FileShares: []*gcpfilev1.FileShareConfig{
			{
				Name:       fs.Fileshare.Name,
				CapacityGb: fs.Fileshare.Size / utils.GB_FACTOR,
			},
		},
		Networks: []*gcpfilev1.NetworkConfig{
			{
				Network: DefaultNetwork,
				Modes:   []string{InternetProtocolModeIpv4},
			},
		},
		Labels: labels,
	}

	log.Infof("Starting CreateInstance on cloud backend")
	log.Debugf("Creating instance %s: location %s, tier %s, capacity %s, labels %v", instanceId,
		fs.Fileshare.AvailabilityZone, instance.Tier, instance.FileShares[0].CapacityGb, instance.Labels)

	op, err := g.instancesService.Create(g.locationURI(g.backend.Region,
		fs.Fileshare.AvailabilityZone), instance).InstanceId(instanceId).Context(ctx).Do()
	if err != nil {
		return nil, fmt.Errorf("createInstance operation failed: %v", err)
	}
	createOp = op
	log.Infof("Create File share, Operation Resource: %s submitted to cloud backend", op.Name)

	return &file.CreateFileShareResponse{
		Fileshare: fs.Fileshare,
	}, nil
}

func (g GcpAdapter) GetFileShare(ctx context.Context, fs *file.GetFileShareRequest) (*file.GetFileShareResponse, error) {
	isCreateOpDone, err := g.isOperationDone(createOp)
	if g.checkForOperation(ctx, isCreateOpDone, createOp, "create", err) != nil {
		log.Error(err)
		return nil, err
	}

	isUpdateOpDone, err := g.isOperationDone(updateOp)
	if g.checkForOperation(ctx, isUpdateOpDone, updateOp, "update", err) != nil {
		log.Error(err)
		return nil, err
	}

	isDeleteOpDone, err := g.isOperationDone(deleteOp)
	if g.checkForOperation(ctx, isDeleteOpDone, deleteOp, "delete", err) != nil {
		log.Error(err)
		return nil, err
	}

	instance, err := g.GetInstance(ctx, fs.Fileshare)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	log.Debugf("Found Instance %+v", instance)

	fileShare, err := g.ParseFileShare(instance)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	if instance.State == "READY" {
		// GCP FileStore Instance exists & ready to use
		fileShare.Status = "available"
		return &file.GetFileShareResponse{
			Fileshare: fileShare,
		}, nil
	} else {
		// GCP FileStore Instance exists but is not ready to use
		return &file.GetFileShareResponse{
			Fileshare: fileShare,
		}, fmt.Errorf("instance %s is in %s", instance.Name, instance.State)
	}
}

func (g GcpAdapter) ListFileShare(ctx context.Context, fs *file.ListFileShareRequest) (*file.ListFileShareResponse, error) {
	instances, err := g.instancesService.List(g.locationURI(g.backend.Region, "-")).Context(ctx).Do()
	if err != nil {
		log.Error(err)
		return nil, err
	}

	var fileshares []*file.FileShare
	for _, instance := range instances.Instances {
		fs, err := g.ParseFileShare(instance)
		if err != nil {
			log.Error(err)
			return nil, err
		}
		fileshares = append(fileshares, fs)
	}

	log.Debugf("List File shares = %+v", fileshares)

	return &file.ListFileShareResponse{
		Fileshares: fileshares,
	}, nil
}

func (g GcpAdapter) UpdatefileShare(ctx context.Context, fs *file.UpdateFileShareRequest) (*file.UpdateFileShareResponse, error) {
	instance, err := g.GetInstance(ctx, fs.Fileshare)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	log.Debugf("Found Instance %+v", instance)

	instanceId := g.getInstanceId(fs.Fileshare.Name)
	instanceUri := g.instanceURI(g.backend.Region, fs.Fileshare.AvailabilityZone, instanceId)

	// Create a file instance for the Patch request.
	instance = &gcpfilev1.Instance{
		Tier: fs.Fileshare.Metadata.Fields["Tier"].GetStringValue(),
		FileShares: []*gcpfilev1.FileShareConfig{
			{
				Name:       fs.Fileshare.Name,
				CapacityGb: fs.Fileshare.Size / utils.GB_FACTOR,
			},
		},
		Networks: []*gcpfilev1.NetworkConfig{
			{
				Network: "default",
				Modes:   []string{"MODE_IPV4"},
			},
		},
	}

	log.Info("Starting PatchInstance operation on cloud backend")
	log.Debugf("Patching instance %v: location %v, tier %v, capacity %v, labels %v", instanceId,
		fs.Fileshare.AvailabilityZone, instance.Tier, instance.FileShares[0].CapacityGb, instance.Labels)

	op, err := g.instancesService.Patch(instanceUri, instance).UpdateMask(fileShareUpdateMask).Context(ctx).Do()
	if err != nil {
		return nil, fmt.Errorf("patch operation failed: %v", err)
	}
	updateOp = op
	log.Infof("Patch File share, Operation Resource = %v", op.Name)

	return &file.UpdateFileShareResponse{
		Fileshare: fs.Fileshare,
	}, nil
}

func (g GcpAdapter) DeleteFileShare(ctx context.Context, fs *file.DeleteFileShareRequest) (*file.DeleteFileShareResponse, error) {
	instance, err := g.GetInstance(ctx, fs.Fileshare)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	log.Debugf("Found Instance %+v", instance)

	log.Infof("Starting DeleteInstance operation on cloud backend")
	instanceId := g.getInstanceId(fs.Fileshare.Name)
	op, err := g.instancesService.Delete(g.instanceURI(g.backend.Region, fs.Fileshare.AvailabilityZone, instanceId)).Context(ctx).Do()
	if err != nil {
		return nil, fmt.Errorf("deleteInstance operation failed: %v", err)
	}
	deleteOp = op
	log.Infof("Delete File share, Operation Resource = %v", op.Name)

	return &file.DeleteFileShareResponse{}, nil
}

func (g *GcpAdapter) ParseFileShare(instance *gcpfilev1.Instance) (*file.FileShare, error) {
	var tags []*file.Tag
	for key, value := range instance.Labels {
		tags = append(tags, &file.Tag{
			Key:   key,
			Value: value,
		})
	}

	meta := map[string]interface{}{
		InstanceResourceName:  instance.Name,
		CreationTimeAtBackend: instance.CreateTime,
		Etag:                  instance.Etag,
		Networks:              instance.Networks,
		State:                 instance.State,
		StatusMessage:         instance.StatusMessage,
	}

	metadata, err := utils.ConvertMapToStruct(meta)
	if err != nil {
		log.Errorf("failed to convert metadata = [%+v] to struct", metadata, err)
		return nil, err
	}

	fileshare := &file.FileShare{
		Size:     instance.FileShares[0].CapacityGb * utils.GB_FACTOR,
		Tags:     tags,
		Metadata: metadata,
	}

	return fileshare, nil
}

func (g *GcpAdapter) GetInstance(ctx context.Context, fs *file.FileShare) (*gcpfilev1.Instance, error) {
	instanceId := g.getInstanceId(fs.Name)
	instance, err := g.instancesService.Get(g.instanceURI(g.backend.Region, fs.AvailabilityZone, instanceId)).Context(ctx).Do()
	if err != nil || instance == nil {
		return nil, fmt.Errorf("failed to get instance %v", err)
	}

	if instance.State != "READY" {
		return instance, fmt.Errorf("instance %s is in %s", instanceId, instance.State)
	}

	return instance, nil
}

func (g *GcpAdapter) checkForOperation(ctx context.Context, isOpDone bool, op *gcpfilev1.Operation, opType string, err error) error {
	if op != nil && !isOpDone && err == nil {
		err = g.waitForOperation(ctx, op)
		if err != nil {
			log.Errorf("wait For %v operation failed: %v", opType, err)
			return err
		}
	}
	return nil
}

func (g *GcpAdapter) waitForOperation(ctx context.Context, op *gcpfilev1.Operation) error {
	return wait.Poll(5*time.Second, 5*time.Minute, func() (bool, error) {
		pollOp, err := g.operationsService.Get(op.Name).Context(ctx).Do()
		if err != nil {
			return false, err
		}
		return g.isOperationDone(pollOp)
	})
}

func (g GcpAdapter) isOperationDone(op *gcpfilev1.Operation) (bool, error) {
	if op == nil {
		return false, nil
	}
	if op.Error != nil {
		return true, fmt.Errorf("operation %s failed (%s): %s", op.Name, op.Error.Code, op.Error.Message)
	}
	return op.Done, nil
}

func (g GcpAdapter) getInstanceId(fsName string) string {
	return fsName + "-instance"
}

func (g GcpAdapter) locationURI(project, location string) string {
	return fmt.Sprintf(locationURIFmt, project, location)
}

func (g GcpAdapter) instanceURI(project, location, name string) string {
	return fmt.Sprintf(instanceURIFmt, project, location, name)
}

func (g GcpAdapter) Close() error {
	// TODO:
	return nil
}
