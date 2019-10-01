package service

import (
	"context"
	"io"
	"time"

	"github.com/micro/go-micro/metadata"
	"github.com/opensds/multi-cloud/api/pkg/common"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	dscommon "github.com/opensds/multi-cloud/s3/pkg/datastore/common"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/driver"
	meta "github.com/opensds/multi-cloud/s3/pkg/meta/types"
	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
	"github.com/opensds/multi-cloud/backend/proto"
	"github.com/opensds/multi-cloud/s3/error"
	. "github.com/opensds/multi-cloud/s3/error"
)

func (s *s3Service) ListObjects(ctx context.Context, in *pb.ListObjectsRequest, out *pb.ListObjectResponse) error {
	log.Infoln("ListObject is called in s3 service.")

	return nil
}

func (s *s3Service) CreateObject(ctx context.Context, in *pb.Object, out *pb.BaseResponse) error {
	log.Infoln("CreateObject is called in s3 service.")

	return nil
}

func (s *s3Service) UpdateObject(ctx context.Context, in *pb.Object, out *pb.BaseResponse) error {
	log.Infoln("PutObject is called in s3 service.")

	return nil
}

type StreamReader struct {
	in pb.S3_PutObjectStream
	curr  int
	req  *pb.PutObjectRequest
}

func (dr *StreamReader) Read(p []byte) (n int, err error) {
	left := len(p)
	for left > 0 {
		if dr.curr == 0 || (dr.req != nil && dr.curr == len(dr.req.Data)){
			dr.req, err = dr.in.Recv()
			if err != nil && err != io.EOF {
				log.Errorln("failed to recv data with err:", err)
				return
			}
			if len(dr.req.Data) == 0 {
				log.Errorln("no data left to read.")
				return
			}
			dr.curr = 0
		}

		copyLen := 0
		if len(dr.req.Data) - dr.curr > left {
			copyLen = left
		} else {
			copyLen = len(dr.req.Data) - dr.curr
		}
		log.Infoln("copy len:", copyLen)
		copy(p[n:], dr.req.Data[dr.curr:(dr.curr + copyLen)])
		dr.curr += copyLen
		left -= copyLen
		n += copyLen
	}
	return
}

func getBackend(ctx context.Context, backedClient backend.BackendService, bucket *meta.Bucket) (*backend.BackendDetail, error) {
	log.Infof("bucketName is %v:\n", bucket.Name)
	backendRep, backendErr := backedClient.ListBackend(ctx, &backendpb.ListBackendRequest{
		Offset: 0,
		Limit:  1,
		Filter: map[string]string{"name": bucket.DefaultLocation}})
	log.Infof("backendErr is %v:", backendErr)
	if backendErr != nil {
		log.Errorf("get backend %s failed.", bucket.DefaultLocation)
		return nil, backendErr
	}
	log.Infof("backendRep is %v:", backendRep)
	backend := backendRep.Backends[0]
	return backend, nil
}

func (s *s3Service) PutObject(ctx context.Context, in pb.S3_PutObjectStream) error {
	log.Infoln("PutObject is called in s3 service.")

	result := &pb.PutObjectResponse{}
	defer in.SendMsg(result)

	var ok bool
	var tenantId string
	var md map[string]string
	md, ok = metadata.FromContext(ctx)
	if !ok {
		log.Error("get metadata from ctx failed.")
		return ErrInternalError
	}

	if tenantId, ok = md[common.CTX_KEY_TENANT_ID]; !ok {
		log.Error("get tenantid failed.")
		return ErrInternalError
	}

	obj := &pb.Object{}
	err := in.RecvMsg(obj)
	if err != nil {
		log.Errorln("failed to get msg with err:", err)
		return ErrInternalError
	}
	obj.TenantId = tenantId
	log.Infof("metadata of object is:%+v\n", obj)
	log.Infof("*********bucket:%s,key:%s,size:%d\n", obj.BucketName, obj.ObjectKey, obj.Size)

	bucket, err := s.MetaStorage.GetBucket(ctx, obj.BucketName, true)
	if err != nil {
		log.Errorln("get bucket failed with err:", err)
		return err
	}
	if bucket == nil {
		log.Infoln("bucket is nil")
	}

	switch bucket.Acl.CannedAcl {
	case "public-read-write":
		break
	default:
		if bucket.TenantId != obj.TenantId {
			return s3error.ErrBucketAccessForbidden
		}
	}

	data := &StreamReader{ in: in}
	var limitedDataReader io.Reader
	if obj.Size > 0 { // request.ContentLength is -1 if length is unknown
		limitedDataReader = io.LimitReader(data, obj.Size)
	} else {
		limitedDataReader = data
	}

	backend, err := getBackend(ctx, s.backendClient, bucket)
	if err != nil {
		log.Errorln("failed to get backend client with err:", err)
		return err
	}

	log.Infoln("bucket location:", obj.Location, " backendtype:", backend.Type,
		" endpoint:", backend.Endpoint)
	bodyMd5 := md["md5Sum"]
	ctx = context.Background()
	ctx = context.WithValue(ctx, dscommon.CONTEXT_KEY_SIZE, obj.Size)
	ctx = context.WithValue(ctx, dscommon.CONTEXT_KEY_MD5, bodyMd5)
	sd, err := driver.CreateStorageDriver(backend.Type, backend)
	if err != nil {
		log.Errorln("failed to create storage. err:", err)
		return nil
	}
	res, err := sd.Put(ctx, limitedDataReader, obj)
	if err != nil {
		log.Errorln("failed to put data. err:", err)
		return err
	}
	oid := res.ObjectId
	bytesWritten := res.Written

	// Should metadata update failed, add `maybeObjectToRecycle` to `RecycleQueue`,
	// so the object in Ceph could be removed asynchronously
	//maybeObjectToRecycle := objectToRecycle{
	//	location: cephCluster.Name,
	//	pool:     poolName,
	//	objectId: oid,
	//}
	if bytesWritten < obj.Size {
		//RecycleQueue <- maybeObjectToRecycle
		log.Warnf("write objects, already written(%d), total size(%d)\n", bytesWritten, obj.Size)
	}
	result.Md5 = res.Etag

	/*if signVerifyReader, ok := data.(*signature.SignVerifyReader); ok {
		credential, err = signVerifyReader.Verify()
		if err != nil {
			RecycleQueue <- maybeObjectToRecycle
			return
		}
	}*/
	// TODO validate bucket policy and fancy ACL
	obj.ObjectId = oid
	obj.LastModified = time.Now().UTC().Unix()
	obj.Etag = res.Etag
	obj.ContentType = md["Content-Type"]
	obj.DeleteMarker = false
	obj.CustomAttributes = md  /* TODO: only reserve http header attr*/
	obj.Type = meta.ObjectTypeNormal

	object := &meta.Object{Object: obj}

	result.LastModified = object.LastModified
	//var nullVerNum uint64
	//nullVerNum, err = yig.checkOldObject(bucketName, objectName, bucket.Versioning)
	//if err != nil {
	//	RecycleQueue <- maybeObjectToRecycle
	//	return
	//}
	//if bucket.Versioning == "Enabled" {
	//	result.VersionId = object.GetVersionId()
	//}
	//// update null version number
	//if bucket.Versioning == "Suspended" {
	//	nullVerNum = uint64(object.LastModifiedTime.UnixNano())
	//}
	//
	//if nullVerNum != 0 {
	//	objMap := &meta.ObjMap{
	//		Name:       objectName,
	//		BucketName: bucketName,
	//	}
	//	err = yig.MetaStorage.PutObject(object, nil, objMap, true)
	//} else {
	//	err = yig.MetaStorage.PutObject(object, nil, nil, true)
	//}

	err = s.MetaStorage.PutObject(ctx, object, nil, nil, true)
	if err != nil {
		log.Errorln("failed to put object meta. err:", err)
		//RecycleQueue <- maybeObjectToRecycle
		return ErrDBError
	}
	if err == nil {
		//b.MetaStorage.Cache.Remove(redis.ObjectTable, obj.OBJECT_CACHE_PREFIX, bucketName+":"+objectKey+":")
		//b.DataCache.Remove(bucketName + ":" + objectKey + ":" + object.GetVersionId())
	}

	return nil
}

func (s *s3Service) GetObject(ctx context.Context, in *pb.GetObjectInput, out *pb.Object) error {
	log.Infoln("GetObject is called in s3 service.")

	return nil
}

func (s *s3Service) DeleteObject(ctx context.Context, in *pb.DeleteObjectInput, out *pb.BaseResponse) error {
	log.Infoln("DeleteObject is called in s3 service.")

	return nil
}
