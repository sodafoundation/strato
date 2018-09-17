package aws

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/micro/go-log"
	. "github.com/opensds/go-panda/s3/pkg/exception"
	pb "github.com/opensds/go-panda/s3/proto"
	"io"
)

type AwsAdapter struct {
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

func Init() *AwsAdapter {
	adap := &AwsAdapter{}
	return adap
}

func (ad *AwsAdapter) PUT(stream io.Reader, object *pb.Object, ctx context.Context) S3Error {
	endpoint := "obs.cn-north-1.myhwclouds.com"
	AccessKeyId := "4X7JQDFTCKUNWFBRYZVC"
	AccessKeySecret := "9hr0ekZgg6vZHulEekTVfWuu1lnPFvpVAJQNHXdn"
	region := "cn-north-1"

	s3aksk := s3Cred{ak: AccessKeyId, sk: AccessKeySecret}
	creds := credentials.NewCredentials(&s3aksk)

	newObjectKey := object.BucketName + "/" + object.ObjectKey

	bucket := "obs-wbtest"
	disableSSL := true
	sess, err := session.NewSession(&aws.Config{
		Region:      &region,
		Endpoint:    &endpoint,
		Credentials: creds,
		DisableSSL:  &disableSSL,
	})
	if err != nil {
		log.Logf("New session failed, err:%v\n", err)
		return InternalError
	}

	if ctx.Value("operation") == "upload" {
		uploader := s3manager.NewUploader(sess)
		out, err := uploader.Upload(&s3manager.UploadInput{
			Bucket: &bucket,
			Key:    &newObjectKey,
			Body:   stream,
		})

		if err != nil {
			log.Logf("Upload to aliyun failed:%v", err)
			return S3Error{Code: 500, Description: "Upload to aliyun failed"}
		}
		object.ServerVersionId = *out.VersionID
	}

	return NoError
}
