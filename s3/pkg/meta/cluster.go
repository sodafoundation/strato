package meta

import (
	. "github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/pkg/helper"
	. "github.com/opensds/multi-cloud/s3/pkg/meta/types"
	"github.com/opensds/multi-cloud/s3/pkg/meta/redis"
	log "github.com/sirupsen/logrus"
)

const (
	CLUSTER_CACHE_PREFIX = "cluster:"
)

func (m *Meta) GetCluster(fsid string, pool string) (cluster Cluster, err error) {
	rowKey := fsid + ObjectNameEnding + pool
	getCluster := func() (c helper.Serializable, err error) {
		log.Info("GetCluster CacheMiss. fsid:", fsid)
		cl, err := m.Db.GetCluster(fsid, pool)
		c = &cl
		return c, err
	}

	toCluster := func(fields map[string]string) (interface{}, error) {
		c := &Cluster{}
		return c.Deserialize(fields)
	}

	c, err := m.Cache.Get(redis.ClusterTable, CLUSTER_CACHE_PREFIX, rowKey, getCluster, toCluster, true)
	if err != nil {
		log.Errorf("failed to get cluster for fsid: %s, err: %v\n", fsid, err)
		return
	}
	cluster, ok := c.(Cluster)
	if !ok {
		err = ErrInternalError
		return
	}
	return cluster, nil
}
