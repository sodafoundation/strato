package ibmcos

import (
	"github.com/opensds/multi-cloud/backend/pkg/utils/constants"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/aws"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/driver"
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
