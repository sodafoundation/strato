package ibmcos

import (
	"github.com/soda/multi-cloud/backend/pkg/utils/constants"
	backendpb "github.com/soda/multi-cloud/backend/proto"
	"github.com/soda/multi-cloud/s3/pkg/datastore/aws"
	"github.com/soda/multi-cloud/s3/pkg/datastore/driver"
)

type IBMCOSDriverFactory struct {
}

func (factory *IBMCOSDriverFactory) CreateDriver(backend *backendpb.BackendDetail) (driver.StorageDriver, error) {
	awss3Fac := &aws.AwsS3DriverFactory{}
	return awss3Fac.CreateDriver(backend)
}

func init() {
	driver.RegisterDriverFactory(constants.BackendTypeIBMCos, &IBMCOSDriverFactory{})
}
