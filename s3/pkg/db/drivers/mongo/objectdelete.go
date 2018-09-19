package mongo

import (
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/micro/go-log"
	. "github.com/opensds/go-panda/s3/pkg/exception"
	pb "github.com/opensds/go-panda/s3/proto"
)

func (ad *adapter) DeleteObject(in *pb.DeleteObjectInput) S3Error {
	//Check if the connctor exist or not
	ss := ad.s.Copy()
	defer ss.Close()

	//Delete it from database
	c := ss.DB(DataBaseName).C(in.Bucket)
	_, err := c.RemoveAll(bson.M{"objectkey": in.Key})
	if err == mgo.ErrNotFound {
		log.Logf("Delete object %s failed, err:the specified object does not exist.", in.Key)
		return NoSuchObject
	} else if err != nil {
		log.Log("Delete object %s from database failed,err:%v.\n", in.Key, err)
		return InternalError
	}
	return NoError
}
