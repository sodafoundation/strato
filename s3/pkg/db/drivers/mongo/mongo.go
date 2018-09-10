package mongo

import(
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	pb "github.com/opensds/go-panda/s3/proto"
	. "github.com/opensds/go-panda/s3/pkg/exception"
	"github.com/micro/go-log"
)


var adap = &adapter{}
var DataBaseName = "__metadatastore"
var BucketMD = "__metadatastore__bucket"



func Init(host string) *adapter {
	//fmt.Println("edps:", deps)
	session,err := mgo.Dial(host)
	if err != nil{
		panic(err)
	}
	//defer session.Close()

	session.SetMode(mgo.Monotonic, true)
	adap.s = session
	adap.userID = "unknown"

	return adap
}

func Exit() {
	adap.s.Close()
}


type adapter struct {
	s *mgo.Session
	userID string
}

func (ad *adapter) CreateBucket(in *pb.Bucket) S3Error {
	ss := ad.s.Copy()
	defer ss.Close()
	out := pb.Bucket{}
	c := ss.DB(DataBaseName).C(BucketMD)
	err := c.Find(bson.M{"name":in.Name}).One(out)
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


func (ad *adapter)GetBucketByName(bucketName string, out *pb.Bucket) S3Error{
	ss := ad.s.Copy()
	defer ss.Close()
	c := ss.DB(DataBaseName).C(BucketMD)
	log.Logf("GetBucketByName: bucketName %s", bucketName)
	err := c.Find(bson.M{"name":bucketName}).One(out)
	if err == mgo.ErrNotFound {
		log.Log("Bucket does not exist.")
		return  NoSuchBucket
	}else if err != nil {
		log.Logf("Get bucket from database failed,err:%v.\n", err)
		return InternalError
	}

	return NoError
}


func (ad *adapter)DeleteBucket(bucketName string) S3Error{
	//Check if the connctor exist or not
	ss := ad.s.Copy()
	defer ss.Close()

	//Delete it from database
	c := ss.DB(DataBaseName).C(BucketMD)
	err := c.Remove(bson.M{"name":bucketName})
	if err == mgo.ErrNotFound{
		log.Log("Delete bucket failed, err:the specified bucket does not exist.")
		return NoSuchBucket
	}else if err != nil {
		log.Log("Delete bucket from database failed,err:%v.\n", err)
		return InternalError
	}
	return NoError
}


func (ad *adapter)UpdateBucket(bucket *pb.Bucket) S3Error{
	//Check if the policy exist or not
	ss := ad.s.Copy()
	defer ss.Close()
	//Update database
	c := ss.DB(DataBaseName).C(BucketMD)
	err := c.Update(bson.M{"name":bucket.Name}, bucket)
	if err == mgo.ErrNotFound{
		log.Log("Update bucket failed: the specified bucket does not exist.")
		return NoSuchBucket
	}else if err != nil {
		log.Log("Update bucket in database failed, err: %v.\n", err)
		return InternalError
	}
	return NoError
}

