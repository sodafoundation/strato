package aws

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/micro/go-log"
	backendpb "github.com/opensds/go-panda/backend/proto"
	. "github.com/opensds/go-panda/s3/pkg/exception"
	pb "github.com/opensds/go-panda/s3/proto"
	"io"
)

type AwsAdapter struct {
	backend *backendpb.BackendDetail
	session *session.Session
}
type s3Cred struct {
	ak string
	sk string
}

func (myc *s3Cred) Retrieve() (credentials.Value, error) {
	cred := credentials.Value{AccessKeyID: myc.ak, SecretAccessKey: myc.sk}
	return cred, nil
}

func (myc *s3Cred) IsExpired() bool {
	return false
}

func Init(backend *backendpb.BackendDetail) *AwsAdapter {
	endpoint := backend.Endpoint
	AccessKeyID := backend.Access
	AccessKeySecret := backend.Security
	region := backend.Region

	s3aksk := s3Cred{ak: AccessKeyID, sk: AccessKeySecret}
	creds := credentials.NewCredentials(&s3aksk)

	disableSSL := true
	sess, err := session.NewSession(&aws.Config{
		Region:      &region,
		Endpoint:    &endpoint,
		Credentials: creds,
		DisableSSL:  &disableSSL,
	})
	if err != nil {
		return nil
	}

	adap := &AwsAdapter{backend: backend, session: sess}
	return adap
}

func (ad *AwsAdapter) PUT(stream io.Reader, object *pb.Object, ctx context.Context) S3Error {

	bucket := ad.backend.BucketName

	newObjectKey := object.BucketName + "/" + object.ObjectKey

	if ctx.Value("operation") == "upload" {
		uploader := s3manager.NewUploader(ad.session)
		_, err := uploader.Upload(&s3manager.UploadInput{
			Bucket: &bucket,
			Key:    &newObjectKey,
			Body:   stream,
		})

		if err != nil {
			log.Logf("Upload to aliyun failed:%v", err)
			return S3Error{Code: 500, Description: "Upload to aliyun failed"}
		}
	}

	return NoError
}

func (ad *AwsAdapter) DELETE(object *pb.DeleteObjectInput, ctx context.Context) S3Error {

	bucket := ad.backend.BucketName

	newObjectKey := object.Bucket + "/" + object.Key

	svc := awss3.New(ad.session)
	deleteInput := awss3.DeleteObjectInput{Bucket: &bucket, Key: &newObjectKey}

	_, err := svc.DeleteObject(&deleteInput)
	if err != nil {
		log.Logf("Delete object failed, err:%v\n", err)
		return InternalError
	}

	return NoError
}
