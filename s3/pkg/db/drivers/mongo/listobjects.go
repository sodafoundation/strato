package mongo

import (
	"github.com/globalsign/mgo/bson"
	"github.com/micro/go-log"
	. "github.com/opensds/go-panda/s3/pkg/exception"
	pb "github.com/opensds/go-panda/s3/proto"
)

func (ad *adapter) ListObjects(in *pb.ListObjectsRequest, out *[]pb.Object) S3Error {
	ss := ad.s.Copy()
	defer ss.Close()
	c := ss.DB(DataBaseName).C(in.Bucket)

	log.Log("Find objects from database...... \n")

	err := c.Find(bson.M{}).All(out)
	//TODO pagination
	if err != nil {
		log.Log("Find objects from database failed, err:%v\n", err)
		return InternalError
	}

	return NoError
}
