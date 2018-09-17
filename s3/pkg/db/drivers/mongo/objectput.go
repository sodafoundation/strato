package mongo

import (
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/micro/go-log"
	. "github.com/opensds/go-panda/s3/pkg/exception"
	pb "github.com/opensds/go-panda/s3/proto"
)

func (ad *adapter) CreateObject(in *pb.Object) S3Error {
	ss := ad.s.Copy()
	defer ss.Close()
	out := pb.Object{}
	c := ss.DB(DataBaseName).C(in.BucketName)
	err := c.Find(bson.M{"name": in.ObjectKey}).One(out)
	if err == mgo.ErrNotFound {
		err := c.Insert(&in)
		if err != nil {
			log.Log("Add object to database failed, err:%v\n", err)
			return InternalError
		}
	} else if err != nil {
		return InternalError
	}

	return NoError
}

func (ad *adapter) UpdateObject(in *pb.Object) S3Error {
	ss := ad.s.Copy()
	defer ss.Close()
	c := ss.DB(DataBaseName).C(in.BucketName)
	err := c.Update(bson.M{"name": in.ObjectKey}, in)
	if err == mgo.ErrNotFound {
		log.Log("Update object to database failed, err:%v\n", err)
		return NoSuchObject
	} else if err != nil {
		log.Log("Update object to database failed, err:%v\n", err)
		return InternalError
	}

	return NoError
}
