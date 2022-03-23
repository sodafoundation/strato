package meta

import (
	"github.com/soda/multi-cloud/s3/pkg/datastore/yig/meta/types"
)

func (m *Meta) GetCluster(fsid, poolName string) (types.Cluster, error) {
	return m.db.GetCluster(fsid, poolName)
}
