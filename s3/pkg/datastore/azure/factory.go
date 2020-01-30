package azure

import (
	"github.com/opensds/multi-cloud/backend/pkg/utils/constants"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/driver"
)

type AzureBlobDriverFactory struct {
}

func (factory *AzureBlobDriverFactory) CreateDriver(backend *backendpb.BackendDetail) (driver.StorageDriver, error) {
	endpoint := backend.Endpoint
	AccessKeyID := backend.Access
	AccessKeySecret := backend.Security
	ad := AzureAdapter{}
	containerURL, err := ad.createContainerURL(endpoint, AccessKeyID, AccessKeySecret)
	if err != nil {
		return nil, err
	}

	adap := &AzureAdapter{backend: backend, containerURL: containerURL}

	return adap, nil
}

func init() {
	driver.RegisterDriverFactory(constants.BackendTypeAzure, &AzureBlobDriverFactory{})
}
