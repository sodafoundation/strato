package mongo

import (
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/micro/go-log"
	. "github.com/opensds/go-panda/s3/pkg/exception"
	pb "github.com/opensds/go-panda/s3/proto"
)

func (ad *adapter) CreateBucket(in *pb.Bucket) S3Error {
	ss := ad.s.Copy()
	defer ss.Close()
	out := pb.Bucket{}
	c := ss.DB(DataBaseName).C(BucketMD)
	err := c.Find(bson.M{"name": in.Name}).One(out)
	if err == mgo.ErrNotFound {
		err := c.Insert(&in)
		if err != nil {
			log.Log("Add bucket to database failed, err:%v\n", err)
			return InternalError
		}
	} else {
		log.Log("The bucket already exists")
		return BucketAlreadyExists
	}

	return NoError
}

func (ad *adapter) UpdateBucket(bucket *pb.Bucket) S3Error {
	//Check if the policy exist or not
	ss := ad.s.Copy()
	defer ss.Close()
	//Update database
	c := ss.DB(DataBaseName).C(BucketMD)
	err := c.Update(bson.M{"name": bucket.Name}, bucket)
	if err == mgo.ErrNotFound {
		log.Log("Update bucket failed: the specified bucket does not exist.")
		return NoSuchBucket
	} else if err != nil {
		log.Log("Update bucket in database failed, err: %v.\n", err)
		return InternalError
	}
	return NoError
}
