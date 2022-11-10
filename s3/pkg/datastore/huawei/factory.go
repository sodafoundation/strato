package hws

import (
	"github.com/soda/multi-cloud/api/pkg/utils/obs"
	"github.com/soda/multi-cloud/backend/pkg/utils/constants"
	backendpb "github.com/soda/multi-cloud/backend/proto"
	"github.com/soda/multi-cloud/s3/pkg/datastore/driver"
)

type HWObsDriverFactory struct {
}

func (cdf *HWObsDriverFactory) CreateDriver(backend *backendpb.BackendDetail) (driver.StorageDriver, error) {
	endpoint := backend.Endpoint
	AccessKeyID := backend.Access
	AccessKeySecret := backend.Security

	client, err := obs.New(AccessKeyID, AccessKeySecret, endpoint)
	if err != nil {
		return nil, err
	}

	adap := &OBSAdapter{backend: backend, client: client}

	return adap, nil
}

func init() {
	driver.RegisterDriverFactory(constants.BackendTypeObs, &HWObsDriverFactory{})
	driver.RegisterDriverFactory(constants.BackendFusionStorage, &HWObsDriverFactory{})
}
