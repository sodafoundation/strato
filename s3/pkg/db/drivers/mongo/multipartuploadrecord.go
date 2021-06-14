package mongo

import (
	"context"
	//"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	log "github.com/sirupsen/logrus"

	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	. "github.com/opensds/multi-cloud/s3/pkg/utils"
	pb "github.com/opensds/multi-cloud/s3/proto"
)

var CollMultipartUploadRecord = "multipartUploadRecords"

func (ad *adapter) AddMultipartUpload(ctx context.Context, record *pb.MultipartUploadRecord) S3Error {
	log.Infof("Add multipart upload: %+v\n", *record)
	session := ad.s.Copy()
	defer session.Close()

	err := session.DB(DataBaseName).C(CollMultipartUploadRecord).Insert(record)
	if err != nil {
		log.Errorf("add multipart upload record[uploadid=%s] to database failed: %v\n", record.UploadId, err)
		return DBError
	}

	log.Infof("add multipart upload record[uploadid=%s] successfully\n", record.UploadId)
	return NoError
}

func (ad *adapter) DeleteMultipartUpload(ctx context.Context, record *pb.MultipartUploadRecord) S3Error {
	log.Infof("Delete multipart upload: %+v\n", *record)
	session := ad.s.Copy()
	defer session.Close()

	m := bson.M{DBKEY_OBJECTKEY: record.ObjectKey, DBKEY_UPLOADID: record.UploadId}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return InternalError
	}

	// objectkey is unique in OpenSDS, uploadid is unique for a specific physical bucket
	err = session.DB(DataBaseName).C(CollMultipartUploadRecord).Remove(m)
	if err != nil && err != mgo.ErrNotFound {
		log.Errorf("delete multipart upload record[uploadid=%s] from database failed: %v\n", record.UploadId, err)
		return DBError
	}

	log.Infof("delete multipart upload record[uploadid=%s] from database sucessfully\n", record.UploadId)
	return NoError
}

/*func (ad *adapter) ListUploadRecords(ctx context.Context, in *pb.ListMultipartUploadRequest,
	out *[]pb.MultipartUploadRecord) S3Error {
	ss := ad.s.Copy()
	defer ss.Close()

	secs := time.Now().Unix() - int64(in.Days*24*60*60)
	log.Infof("list upload records here: bucket=%s, prefix=%s, daysAfterInitiation=%d, limit=%d, offset=%d, secs=%d\n",
		in.Bucket, in.Prefix, in.Days, in.Limit, in.Offset, secs)

	m := bson.M{DBKEY_BUCKET: in.Bucket, DBKEY_INITTIME: bson.M{"$lte": secs}, DBKEY_OBJECTKEY: bson.M{"$regex": "^" +
		in.Prefix}}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return InternalError
	}

	err = ss.DB(DataBaseName).C(CollMultipartUploadRecord).Find(m).Skip(int(in.Offset)).Limit(int(in.Limit)).All(out)
	if err != nil && err != mgo.ErrNotFound {
		log.Errorf("list upload records failed:%v\n", err)
		return DBError
	}

	return NoError
}*/
