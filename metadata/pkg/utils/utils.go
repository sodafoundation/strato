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

package utils

import (
	"context"

	backend "github.com/opensds/multi-cloud/backend/proto"
	"github.com/opensds/multi-cloud/metadata/pkg/model"
	pb "github.com/opensds/multi-cloud/metadata/proto"
	log "github.com/sirupsen/logrus"
)

func GetBackends(unPaginatedResult []*model.MetaBackend) []*pb.BackendMetadata {
	var protoBackends []*pb.BackendMetadata
	for _, backend := range unPaginatedResult {
		var protoBuckets []*pb.BucketMetadata
		protoBuckets = GetBuckets(backend, protoBuckets)
		protoBackend := &pb.BackendMetadata{
			Id:                      string(backend.Id),
			BackendName:             backend.BackendName,
			Region:                  backend.Region,
			Type:                    backend.Type,
			Buckets:                 protoBuckets,
			NumberOfBuckets:         backend.NumberOfBuckets,
			NumberOfFilteredBuckets: backend.NumberOfFilteredBuckets,
		}
		protoBackends = append(protoBackends, protoBackend)
	}
	return protoBackends
}

func GetBuckets(backend *model.MetaBackend, protoBuckets []*pb.BucketMetadata) []*pb.BucketMetadata {
	for _, bucket := range backend.Buckets {

		protoObjects := GetObjects(bucket)

		protoBucket := &pb.BucketMetadata{
			Name:                    bucket.Name,
			Type:                    bucket.Type,
			Region:                  bucket.Region,
			TotalSizeInBytes:        bucket.TotalSize,
			FilteredBucketSize:      bucket.FilteredBucketSize,
			NumberOfObjects:         int32(bucket.NumberOfObjects),
			NumberOfFilteredObjects: int32(bucket.NumberOfFilteredObjects),
			CreationDate:            bucket.CreationDate.String(),
			Tags:                    bucket.BucketTags,
			Objects:                 protoObjects,
		}
		protoBuckets = append(protoBuckets, protoBucket)
	}
	return protoBuckets
}

func GetObjects(bucket *model.MetaBucket) []*pb.ObjectMetadata {
	var protoObjects []*pb.ObjectMetadata

	for _, object := range bucket.Objects {
		var expiresDateStr string
		if object.ExpiresDate == nil {
			expiresDateStr = ""
		} else {
			expiresDateStr = object.ExpiresDate.String()
		}

		protoObject := &pb.ObjectMetadata{
			Name:                 object.ObjectName,
			LastModifiedDate:     object.LastModifiedDate.String(),
			SizeInBytes:          int32(object.Size),
			BucketName:           bucket.Name,
			Type:                 object.ObjectType,
			ServerSideEncryption: object.ServerSideEncryption,
			ExpiresDate:          expiresDateStr,
			GrantControl:         object.GrantControl,
			VersionId:            string(object.VersionId),
			RedirectLocation:     object.RedirectLocation,
			ReplicationStatus:    object.ReplicationStatus,
			Tags:                 object.ObjectTags,
			Metadata:             object.Metadata,
		}
		protoObjects = append(protoObjects, protoObject)
	}
	return protoObjects
}

func GetBackend(ctx context.Context, backedClient backend.BackendService, backendID string) (*backend.BackendDetail,
	error) {
	backend, err := backedClient.GetBackend(ctx, &backend.GetBackendRequest{
		Id: backendID,
	})
	if err != nil {
		log.Errorf("get backend %s failed.", backendID)
		return nil, err
	}
	return backend.Backend, nil
}

func ListBackend(ctx context.Context, backedClient backend.BackendService) ([]*backend.BackendDetail,
	error) {
	backend, err := backedClient.ListBackend(ctx, &backend.ListBackendRequest{})
	if err != nil {
		log.Errorf("failed to list backends for sync metadata")
		return nil, err
	}
	return backend.Backends, nil
}
