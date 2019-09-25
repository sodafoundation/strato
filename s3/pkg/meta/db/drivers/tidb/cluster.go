package tidbclient

import (
	. "github.com/opensds/multi-cloud/s3/pkg/meta/types"
)

//cluster
func (t *TidbClient) GetCluster(fsid, pool string) (cluster Cluster, err error) {
	sqltext := "select fsid,pool,weight from cluster where fsid=? and pool=?"
	err = t.Client.QueryRow(sqltext, fsid, pool).Scan(
		&cluster.Fsid,
		&cluster.Pool,
		&cluster.Weight,
	)
	return
}
