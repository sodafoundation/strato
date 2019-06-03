package mongo

import (
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/micro/go-log"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	. "github.com/opensds/multi-cloud/s3/pkg/utils"
	pb "github.com/opensds/multi-cloud/s3/proto"
)

var CollMultipartUploadRecord = "multipartUploadRecords"

func (ad *adapter) AddMultipartUpload(record *pb.MultipartUploadRecord) S3Error {
	log.Logf("Add multipart upload: %+v\n", *record)
	session := ad.s.Copy()
	defer session.Close()

	collection := session.DB(DataBaseName).C(CollMultipartUploadRecord)
	err := collection.Insert(record)
	if err != nil {
		log.Logf("add multipart upload record[uploadid=%s] to database failed: %v\n", record.UploadId, err)
		return DBError
	}

	log.Logf("add multipart upload record[uploadid=%s] successfully\n", record.UploadId)
	return NoError
}

func (ad *adapter) DeleteMultipartUpload(record *pb.MultipartUploadRecord) S3Error {
	log.Logf("Delete multipart upload: %+v\n", *record)
	session := ad.s.Copy()
	defer session.Close()

	collection := session.DB(DataBaseName).C(CollMultipartUploadRecord)
	// objectkey is unique in OpenSDS, uploadid is unique for a specific physical bucket
	err := collection.Remove(bson.M{DBKEY_OBJECTKEY: record.ObjectKey, DBKEY_UPLOADID: record.UploadId})
	if err != nil && err != mgo.ErrNotFound {
		log.Logf("delete multipart upload record[uploadid=%s] from database failed: %v\n", record.UploadId, err)
		return DBError
	}

	log.Logf("delete multipart upload record[uploadid=%s] from database sucessfully\n", record.UploadId)
	return NoError
}

func (ad *adapter) ListUploadRecords(in *pb.ListMultipartUploadRequest, out *[]pb.MultipartUploadRecord) S3Error {
	ss := ad.s.Copy()
	defer ss.Close()

	secs := time.Now().Unix() - int64(in.Days*24*60*60)
	log.Logf("list upload records here: bucket=%s, prefix=%s, daysAfterInitiation=%d, limit=%d, offset=%d, secs=%d\n",
		in.Bucket, in.Prefix, in.Days, in.Limit, in.Offset, secs)

	c := ss.DB(DataBaseName).C(CollMultipartUploadRecord)
	filter := bson.M{"bucket": in.Bucket, "inittime": bson.M{"$lte": secs}, "objectkey": bson.M{"$regex": "^" + in.Prefix}}
	err := c.Find(filter).Skip(int(in.Offset)).Limit(int(in.Limit)).All(out)
	if err != nil && err != mgo.ErrNotFound {
		log.Logf("list upload records failed:%v\n", err)
		return DBError
	}

	return NoError
}
