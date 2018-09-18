package hws

import (
	"context"
	"github.com/micro/go-log"
	backendpb "github.com/opensds/go-panda/backend/proto"
	. "github.com/opensds/go-panda/s3/pkg/exception"
	pb "github.com/opensds/go-panda/s3/proto"
	"io"
	"obs"
)

type OBSAdapter struct {
	backend *backendpb.BackendDetail
	client  *obs.ObsClient
}

func Init(backend *backendpb.BackendDetail) *OBSAdapter {
	endpoint := backend.Endpoint
	AccessKeyID := backend.Access
	AccessKeySecret := backend.Security

	client, err := obs.New(AccessKeyID, AccessKeySecret, endpoint)

	if err != nil {
		log.Logf("Access obs failed:%v", err)
		return nil
	}

	adap := &OBSAdapter{backend: backend, client: client}
	return adap
}

func (ad *OBSAdapter) PUT(stream io.Reader, object *pb.Object, ctx context.Context) S3Error {

	bucket := ad.backend.BucketName
	if ctx.Value("operation") == "upload" {
		input := &obs.PutObjectInput{}
		input.Bucket = bucket
		input.Key = object.BucketName + "/" + object.ObjectKey
		input.Body = stream

		out, err := ad.client.PutObject(input)

		if err != nil {
			log.Logf("Upload to obs failed:%v", err)
			return S3Error{Code: 500, Description: "Upload to obs failed"}
		}
		log.Logf("Upload %s to obs successfully.", out.VersionId)
	}

	return NoError
}

func (ad *OBSAdapter) DELETE(object *pb.DeleteObjectInput, ctx context.Context) S3Error {

	newObjectKey := object.Bucket + "/" + object.Key
	deleteObjectInput := obs.DeleteObjectInput{Bucket: ad.backend.BucketName, Key: newObjectKey}
	_, err := ad.client.DeleteObject(&deleteObjectInput)
	if err != nil {
		log.Logf("Delete  object failed:%v", err)
		return InternalError
	}

	return NoError
}
