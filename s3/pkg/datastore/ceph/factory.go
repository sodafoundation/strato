package ceph

import (
	"github.com/opensds/multi-cloud/backend/pkg/utils/constants"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/driver"
	"github.com/webrtcn/s3client"
)

type CephS3DriverFactory struct {
}

func (cdf *CephS3DriverFactory) CreateDriver(backend *backendpb.BackendDetail) (driver.StorageDriver, error) {
	endpoint := backend.Endpoint
	AccessKeyID := backend.Access
	AccessKeySecret := backend.Security
	sess := s3client.NewClient(endpoint, AccessKeyID, AccessKeySecret)
	adap := &CephAdapter{backend: backend, session: sess}

	return adap, nil
}

func init() {
	driver.RegisterDriverFactory(constants.BackendTypeCeph, &CephS3DriverFactory{})
}
