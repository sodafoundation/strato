package cloudfactory

import (
	"context"

	"github.com/opensds/multi-cloud/metadata/pkg/model"
	pb "github.com/opensds/multi-cloud/metadata/proto"
)

// define the common driver interface for io.

type CloudDriver interface {
	SyncMetadata(ctx context.Context, input *pb.SyncMetadataRequest) error
	ObjectList(bucket *model.MetaBucket) error
	BucketList() ([]model.MetaBucket, error)
	DownloadObject()
	//BackendCheck(ctx context.Context, backendDetail *pb.BackendDetailS3) error
}
