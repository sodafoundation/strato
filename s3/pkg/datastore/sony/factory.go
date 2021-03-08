package sony

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/opensds/multi-cloud/backend/pkg/utils/constants"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	awsdk "github.com/opensds/multi-cloud/s3/pkg/datastore/aws"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/driver"
)

type SonyS3DriverFactory struct {
}

func (factory *SonyS3DriverFactory) CreateDriver(backend *backendpb.BackendDetail) (driver.StorageDriver, error) {
	endpoint := backend.Endpoint
	AccessKeyID := backend.Access
	AccessKeySecret := backend.Security

	s3aksk := awsdk.S3Cred{Ak: AccessKeyID, Sk: AccessKeySecret}
	creds := credentials.NewCredentials(&s3aksk)

	sess, err := session.NewSession(&aws.Config{
		S3ForcePathStyle: aws.Bool(true),
		Region:           aws.String("us-east-1"),
		Endpoint:         &endpoint,
		Credentials:      creds,
		DisableSSL:       aws.Bool(false),
	})
	if err != nil {
		return nil, err
	}

	adap := &awsdk.AwsAdapter{Backend: backend, Session: sess}

	return adap, nil
}

func init() {
	driver.RegisterDriverFactory(constants.BackendTypeSonyODA, &SonyS3DriverFactory{})
}
