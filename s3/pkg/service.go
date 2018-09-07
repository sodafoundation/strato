package pkg

import (
	"context"

	"github.com/micro/go-log"
	pb "github.com/opensds/go-panda/s3/proto"
)

type s3Service struct{}


func (b *s3Service) ListBuckets(ctx context.Context, in *pb.BaseRequest, out *pb.ListBucketsResponse) error {
	log.Log("ListBuckets is called in s3 service.")
	return nil
}

func (b *s3Service) CreateBucket(ctx context.Context, in *pb.Bucket, out *pb.BaseResponse) error {
	log.Log("CreateBucket is called in s3 service.")
	out.Msg = "Create bucket successfully."
	return nil
}


func (b *s3Service) GetBucket(ctx context.Context, in *pb.Bucket, out *pb.Bucket) error {
	log.Log("CreateBucket is called in s3 service.")
	out.Name = in.Name
	out.OwnerDisplayName = "testOwner"
	return nil
}

func (b *s3Service) DeleteBucket(ctx context.Context, in *pb.Bucket, out *pb.BaseResponse) error {
	log.Log("DeleteBucket is called in s3 service.")
	out.Msg = "Delete bucket successfully."
	return nil
}

func (b *s3Service) ListObjects(ctx context.Context, in *pb.Object, out *pb.ListObjectResponse) error {
	log.Log("PutObject is called in s3 service.")
	return nil
}

func (b *s3Service) PutObject(ctx context.Context, in *pb.Object, out *pb.BaseResponse) error {
	log.Log("PutObject is called in s3 service.")
	out.Msg = "Create bucket successfully."
	return nil
}

func (b *s3Service) GetObject(ctx context.Context, in *pb.Object, out *pb.Object) error {
	log.Log("GetObject is called in s3 service.")
	out.ObjectKey = in.ObjectKey
	out.BucketName = in.BucketName
	return nil
}

func (b *s3Service) DeleteObject(ctx context.Context, in *pb.Object, out *pb.BaseResponse) error {
	log.Log("PutObject is called in s3 service.")
	out.Msg = "Create bucket successfully."
	return nil
}

func NewS3Service() pb.S3Handler {
	return &s3Service{}
}
