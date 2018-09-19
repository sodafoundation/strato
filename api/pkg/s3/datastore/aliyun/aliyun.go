package aliyun

import (
	"context"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/micro/go-log"
	backendpb "github.com/opensds/go-panda/backend/proto"
	. "github.com/opensds/go-panda/s3/pkg/exception"
	pb "github.com/opensds/go-panda/s3/proto"
	"io"
)

type AliyunAdapter struct {
	backend *backendpb.BackendDetail
	client  *oss.Client
}

func Init(backend *backendpb.BackendDetail) *AliyunAdapter {
	endpoint := backend.Endpoint
	AccessKeyId := backend.Access
	AccessKeySecret := backend.Security

	client, err := oss.New(endpoint, AccessKeyId, AccessKeySecret)
	if err != nil {
		log.Logf("Access aliyun failed:%v", err)
		return nil
	}

	adap := &AliyunAdapter{backend: backend, client: client}
	return adap
}

func (ad *AliyunAdapter) PUT(stream io.Reader, object *pb.Object, ctx context.Context) S3Error {

	bucketName := ad.backend.BucketName

	bucket, err := ad.client.Bucket(bucketName)
	if err != nil {
		log.Logf("Access bucket failed:%v", err)
		return S3Error{Code: 500, Description: "Access bucket failed"}
	}

	if ctx.Value("operation") == "upload" {
		newObjectKey := object.BucketName + "/" + object.ObjectKey

		err = bucket.PutObject(newObjectKey, stream)

		if err != nil {
			log.Logf("Upload to aliyun failed:%v", err)
			return S3Error{Code: 500, Description: "Upload to aliyun failed"}
		}
	}

	return NoError
}

func (ad *AliyunAdapter) DELETE(object *pb.DeleteObjectInput, ctx context.Context) S3Error {

	newObjectKey := object.Bucket + "/" + object.Key
	bucketName := ad.backend.BucketName

	bucket, err := ad.client.Bucket(bucketName)
	if err != nil {
		log.Logf("Access bucket failed:%v", err)
		return S3Error{Code: 500, Description: "Access bucket failed"}
	}

	deleteErr := bucket.DeleteObject(newObjectKey)
	if deleteErr != nil {
		log.Logf("Delete object failed:%v", err)
		return S3Error{Code: 500, Description: "Delete object failed"}
	}

	log.Logf("Delete object %s from aliyun successfully.\n", newObjectKey)
	return NoError
}
