package driver

import (
	"context"
	"io"

	dscommon "github.com/soda/multi-cloud/s3/pkg/datastore/common"
	"github.com/soda/multi-cloud/s3/pkg/model"
	pb "github.com/soda/multi-cloud/s3/proto"
)

// define the common driver interface for io.

type StorageDriver interface {
	BucketCreate(ctx context.Context, input *pb.Bucket) error
	Put(ctx context.Context, stream io.Reader, object *pb.Object) (dscommon.PutResult, error)
	Get(ctx context.Context, object *pb.Object, start int64, end int64) (io.ReadCloser, error)
	Delete(ctx context.Context, object *pb.DeleteObjectInput) error
	BucketDelete(ctx context.Context, in *pb.Bucket) error
	// TODO AppendObject
	Copy(ctx context.Context, stream io.Reader, target *pb.Object) (dscommon.PutResult, error)

	InitMultipartUpload(ctx context.Context, object *pb.Object) (*pb.MultipartUpload, error)
	UploadPart(ctx context.Context, stream io.Reader, multipartUpload *pb.MultipartUpload,
		partNumber int64, upBytes int64) (*model.UploadPartResult, error)
	CompleteMultipartUpload(ctx context.Context, multipartUpload *pb.MultipartUpload,
		completeUpload *model.CompleteMultipartUpload) (*model.CompleteMultipartUploadResult, error)
	AbortMultipartUpload(ctx context.Context, multipartUpload *pb.MultipartUpload) error
	ListParts(ctx context.Context, multipartUpload *pb.ListParts) (*model.ListPartsOutput, error)
	BackendCheck(ctx context.Context, backendDetail *pb.BackendDetailS3) error
	Restore(ctx context.Context, restoreObj *pb.Restore) error
	// Close: cleanup when driver needs to be stopped.
	Close() error

	// change storage class
	ChangeStorageClass(ctx context.Context, object *pb.Object, newClass *string) error
}
