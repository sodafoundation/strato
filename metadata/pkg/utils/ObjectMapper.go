package utils

import (
	"github.com/opensds/multi-cloud/metadata/pkg/model"
	"github.com/opensds/multi-cloud/metadata/proto"

	pb "github.com/opensds/multi-cloud/metadata/proto"
)

func GetBackends(unPaginatedResult []*model.MetaBackend) []*pb.BackendMetadata {
	var protoBackends []*pb.BackendMetadata
	for _, backend := range unPaginatedResult {
		var protoBuckets []*pb.BucketMetadata
		protoBuckets = GetBuckets(backend, protoBuckets)
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

func GetBuckets(backend *model.MetaBackend, protoBuckets []*pb.BucketMetadata) []*pb.BucketMetadata {
	for _, bucket := range backend.Buckets {

		protoObjects := GetObjects(bucket)

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
func GetObjects(bucket model.MetaBucket) []*proto.ObjectMetadata {
	var protoObjects []*proto.ObjectMetadata

	for _, object := range bucket.Objects {
		protoObject := &proto.ObjectMetadata{
			Name:                 object.ObjectName,
			LastModifiedDate:     object.LastModifiedDate.String(),
			SizeInBytes:          int32(object.Size),
			BucketName:           bucket.Name,
			Type:                 object.ObjectType,
			ServerSideEncryption: object.ServerSideEncryption,
			ExpiresDate:          object.ExpiresDate.String(),
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
