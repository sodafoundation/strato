package mongo

import (
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/micro/go-log"
	. "github.com/opensds/go-panda/s3/pkg/exception"
	pb "github.com/opensds/go-panda/s3/proto"
)

func (ad *adapter) GetBucketByName(bucketName string, out *pb.Bucket) S3Error {
	ss := ad.s.Copy()
	defer ss.Close()
	c := ss.DB(DataBaseName).C(BucketMD)
	log.Logf("GetBucketByName: bucketName %s", bucketName)
	err := c.Find(bson.M{"name": bucketName}).One(out)
	if err == mgo.ErrNotFound {
		log.Log("Bucket does not exist.")
		return NoSuchBucket
	} else if err != nil {
		log.Logf("Get bucket from database failed,err:%v.\n", err)
		return InternalError
	}

	return NoError
}
