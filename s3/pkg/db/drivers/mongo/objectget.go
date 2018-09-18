package mongo

import (
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/micro/go-log"
	. "github.com/opensds/go-panda/s3/pkg/exception"
	pb "github.com/opensds/go-panda/s3/proto"
)

func (ad *adapter) GetObject(in *pb.GetObjectInput, out *pb.Object) S3Error {
	ss := ad.s.Copy()
	defer ss.Close()
	c := ss.DB(DataBaseName).C(in.Bucket)

	log.Log("Find objects from database...... \n")

	err := c.Find(bson.M{"ObjectKey": in.Key}).One(&out)
	if err == mgo.ErrNotFound {
		log.Log("Object does not exist.")
		return NoSuchObject
	} else if err != nil {
		log.Log("Find objects from database failed, err:%v\n", err)
		return InternalError
	}

	return NoError
}
