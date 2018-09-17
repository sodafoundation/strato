package mongo

import (
	"github.com/globalsign/mgo/bson"
	"github.com/micro/go-log"
	. "github.com/opensds/go-panda/s3/pkg/exception"
	pb "github.com/opensds/go-panda/s3/proto"
)

func (ad *adapter) ListBuckets(in *pb.BaseRequest) ([]pb.Bucket, S3Error) {
	ss := ad.s.Copy()
	defer ss.Close()
	buckets := []pb.Bucket{}
	c := ss.DB(DataBaseName).C(BucketMD)

	log.Log("Find buckets from database...... \n")

	err := c.Find(bson.M{"owner": in.Id}).All(&buckets)
	if err != nil {
		log.Log("Find buckets from database failed, err:%v\n", err)
		return buckets, InternalError
	}

	return buckets, NoError
}
