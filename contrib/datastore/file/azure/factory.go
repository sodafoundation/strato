package azure

import (
	"github.com/micro/go-micro/v2/util/log"
	"github.com/opensds/multi-cloud/backend/pkg/utils/constants"
	"github.com/opensds/multi-cloud/contrib/datastore/drivers"

	backendpb "github.com/opensds/multi-cloud/backend/proto"
)

type AzureFSDriverFactory struct {
}

func (factory *AzureFSDriverFactory) CreateFileStorageDriver(backend *backendpb.BackendDetail) (driver.FileStorageDriver, error) {
	log.Infof("Entered to create azure file share driver")
	endpoint := backend.Endpoint
	AccessKeyID := backend.Access
	AccessKeySecret := backend.Security
	ad := AzureAdapter{}
	pipeline, err := ad.createPipeline(endpoint, AccessKeyID, AccessKeySecret)
	if err != nil {
		return nil, err
	}

	adapter := &AzureAdapter{backend: backend, pipeline: pipeline}

	return adapter, nil
}

func init() {
	driver.RegisterDriverFactory(constants.BackendTypeAzureFile, &AzureFSDriverFactory{})
}
