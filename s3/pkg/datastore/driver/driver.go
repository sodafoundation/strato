package driver

import (
	"context"
	"io"

	dscommon "github.com/opensds/multi-cloud/s3/pkg/datastore/common"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	pb "github.com/opensds/multi-cloud/s3/proto"
)

// define the common driver interface for io.

type StorageDriver interface {
	Put(ctx context.Context, stream io.Reader, object *pb.Object) (dscommon.PutResult, error)
	Get(ctx context.Context, object *pb.Object, start int64, end int64) (io.ReadCloser, error)
	Delete(ctx context.Context, object *pb.DeleteObjectInput) error
	// TODO AppendObject
	// TODO CopyObject

	InitMultipartUpload(ctx context.Context, object *pb.Object) (*pb.MultipartUpload, error)
	UploadPart(ctx context.Context, stream io.Reader, multipartUpload *pb.MultipartUpload,
		partNumber int64, upBytes int64) (*model.UploadPartResult, error)
	// TODO CopyPart
	CompleteMultipartUpload(ctx context.Context, multipartUpload *pb.MultipartUpload,
		completeUpload *model.CompleteMultipartUpload) (*model.CompleteMultipartUploadResult, error)
	AbortMultipartUpload(ctx context.Context, multipartUpload *pb.MultipartUpload) error
	// Close: cleanup when driver needs to be stopped.
	Close() error
}
