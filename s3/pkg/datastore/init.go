package datastore

import (
	//_ "github.com/opensds/multi-cloud/s3/pkg/datastore/ceph"
	_ "github.com/opensds/multi-cloud/s3/pkg/datastore/yig"
	_ "github.com/opensds/multi-cloud/s3/pkg/datastore/aws"
	_ "github.com/opensds/multi-cloud/s3/pkg/datastore/huawei"
)
