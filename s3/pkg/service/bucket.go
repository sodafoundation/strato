package service

import (
	"context"

	"github.com/opensds/multi-cloud/api/pkg/s3"
	. "github.com/opensds/multi-cloud/s3/error"
	. "github.com/opensds/multi-cloud/s3/pkg/meta/types"
	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

func (s *s3Service) ListBuckets(ctx context.Context, in *pb.BaseRequest, out *pb.ListBucketsResponse) error {
	log.Info("ListBuckets is called in s3 service.")
	//buckets := []pb.Bucket{}
	buckets, err := s.MetaStorage.Db.GetBuckets(ctx)
	if err != nil {
		log.Errorf("list buckets failed, err:%v\n", err)
		s3err, ok := err.(S3ErrorCode)
		if ok {
			out.ErrorCode = int32(ErrorCodeResponse[s3err].HttpStatusCode)
		} else {
			out.ErrorCode = int32(ErrorCodeResponse[ErrInternalError].HttpStatusCode)
		}
		return err
	}

	// TODO: paging list
	for j := 0; j < len(buckets); j++ {
		if buckets[j].Deleted != true {
			out.Buckets = append(out.Buckets, &pb.Bucket{
				Name:            buckets[j].Name,
				TenantId:        buckets[j].TenantId,
				CreateTime:      buckets[j].CreateTime,
				Usages:          buckets[j].Usages,
				Tier:            buckets[j].Tier,
				DefaultLocation: buckets[j].DefaultLocation,
			})
		}
	}

	log.Infof("out.Buckets:%+v\n", out.Buckets)
	return nil
}

func (s *s3Service) CreateBucket(ctx context.Context, in *pb.Bucket, out *pb.BaseResponse) error {
	log.Infof("CreateBucket is called in s3 service, in:%+v\n", in)
	var err error
	defer HandleS3Error(err, out)

	bucketName := in.Name
	if err := s3.CheckValidBucketName(bucketName); err != nil {
		err = ErrInvalidBucketName
		return err
	}

	//credential := ctx.Value(s3.RequestContextKey).(s3.RequestContext).Credential
	processed, err := s.MetaStorage.Db.CheckAndPutBucket(ctx, &Bucket{Bucket: in})
	if err != nil {
		log.Error("Error making checkandput: ", err)
		return err
	}
	log.Infof("create bucket[%s] in database succeed, processed=%v.\n", in.Name, processed)
	if !processed { // bucket already exists, return accurate message
		/*bucket*/ _, err := s.MetaStorage.GetBucket(ctx, bucketName, false)
		if err == nil {
			log.Error("Error get bucket: ", bucketName, ", with error", err)
			err = ErrBucketAlreadyExists
		}
		/*if bucket.OwnerId == credential.UserId {
			return ErrBucketAlreadyOwnedByYou
		} else {
			return ErrBucketAlreadyExists
		}*/
	}
	/*err = b.MetaStorage.Db.AddBucketForUser(bucketName, in.OwnerId)
	if err != nil { // roll back bucket table, i.e. remove inserted bucket
		log.Error("Error AddBucketForUser: ", err)
		err = b.MetaStorage.Db.DeleteBucket(&types.Bucket{Bucket: in})
		if err != nil {
			log.Error("Error deleting: ", err)
			helper.Logger.Println(5, "Leaving junk bucket unremoved: ", bucketName)
			return err
		}
	}

	if err == nil {
		b.MetaStorage.Cache.Remove(redis.UserTable, meta.BUCKET_CACHE_PREFIX, in.OwnerId)
	}*/

	return err
}

func (s *s3Service) GetBucket(ctx context.Context, in *pb.BaseRequest, out *pb.Bucket) error {
	log.Infof("GetBucket %s is called in s3 service.", in.Id)

	bucket, err := s.MetaStorage.GetBucket(ctx, in.Id, false)
	if err != nil {
		log.Errorf("get bucket[%s] failed, err:%v\n", in.Id, err)
		return err
	}

	out = &pb.Bucket{
		Id:              bucket.Id,
		Name:            bucket.Name,
		TenantId:        bucket.TenantId,
		UserId:          bucket.UserId,
		Acl:             bucket.Acl,
		CreateTime:      bucket.CreateTime,
		Deleted:         bucket.Deleted,
		DefaultLocation: bucket.DefaultLocation,
		Tier:            bucket.Tier,
		Usages:          bucket.Usages,
	}

	return nil
}

func (s *s3Service) DeleteBucket(ctx context.Context, in *pb.Bucket, out *pb.BaseResponse) error {
	bucketName := in.Name
	log.Infof("DeleteBucket is called in s3 service, bucketName is %s.\n", bucketName)
	var err error
	defer HandleS3Error(err, out)

	bucket, err := s.MetaStorage.GetBucket(ctx, bucketName, false)
	if err != nil {
		log.Errorf("get bucket failed, err:%+v\n", err)
		return err
	}

	// Check if bucket is empty
	objs, _, _, _, _, err := s.MetaStorage.Db.ListObjects(ctx, bucketName, "", "", "",
		"", false, 1)
	if err != nil {
		log.Errorf("list objects failed, err:%v\n", err)
		return err
	}
	if len(objs) != 0 {
		log.Errorf("bucket[%s] is not empty.\n", bucketName)
		err = ErrBucketNotEmpty
		return err
	}
	err = s.MetaStorage.Db.DeleteBucket(ctx, bucket)
	if err != nil {
		return err
	}

	/*err = s.MetaStorage.RemoveBucketForUser(bucketName, in.TenantId)
	if err != nil { // roll back bucket table, i.e. re-add removed bucket entry
		err = s.MetaStorage.Client.AddBucketForUser(bucketName, in.TenantId)
		if err != nil {
			return err
		}
	}

	if err == nil {
		yig.MetaStorage.Cache.Remove(redis.UserTable, meta.USER_CACHE_PREFIX, credential.UserId)
		yig.MetaStorage.Cache.Remove(redis.BucketTable, meta.BUCKET_CACHE_PREFIX, bucketName)
	}

	if bucket.LC.Rule != nil {
		err = yig.MetaStorage.RemoveBucketFromLifeCycle(bucket)
		if err != nil {
			yig.Logger.Println(5, "Error remove bucket from lifeCycle: ", err)
		}
	}*/

	return nil
}
