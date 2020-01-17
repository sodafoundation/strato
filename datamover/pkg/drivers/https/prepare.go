package migration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	flowtype "github.com/opensds/multi-cloud/dataflow/pkg/model"
	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
	pb "github.com/opensds/multi-cloud/datamover/proto"
	osdss3 "github.com/opensds/multi-cloud/s3/proto"
	s3 "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

func GetCtxTimeout(key string, min, max, def int64) time.Duration {
	// default value
	tmout := time.Duration(def) * time.Second

	tmoutCfg, err := strconv.ParseInt(os.Getenv(key), 10, 64)
	if err != nil || tmoutCfg < min || tmoutCfg > max {
		tmoutCfg = int64(JOB_RUN_TIME_MAX)
	}
	durStr := fmt.Sprintf("%ds", tmoutCfg)
	log.Debugf("Vaule of %s is: %d seconds, durStr:%s.\n", key, tmoutCfg, durStr)
	val, err := time.ParseDuration(durStr)
	if err == nil {
		tmout = val
	}

	log.Infof("tmout=%v\n", tmout)
	return tmout
}

func getObjs(ctx context.Context, in *pb.RunJobRequest, marker string, limit int32) ([]*osdss3.Object, error) {
	switch in.SourceConn.Type {
	case flowtype.STOR_TYPE_OPENSDS:
		return getOsdsS3Objs(ctx, in, marker, limit)
	default:
		log.Errorf("unsupport storage type:%v\n", in.SourceConn.Type)
	}

	return nil, errors.New(DMERR_InternalError)
}

func countOsdsS3Objs(ctx context.Context, in *pb.RunJobRequest) (count, size int64, err error) {
	log.Debugf("count objects of bucket[%s].\n", in.SourceConn.BucketName)

	req := osdss3.ListObjectsRequest{Bucket: in.SourceConn.BucketName}
	if in.GetFilt() != nil && len(in.Filt.Prefix) > 0 {
		req.Prefix = in.Filt.Prefix
	}

	rsp, err := s3client.CountObjects(ctx, &req)
	if err != nil {
		log.Errorf("err: %v\n", err)
		return 0, 0, errors.New(DMERR_InternalError)
	}

	log.Debugf("count objects of bucket[%s]: count=%d,size=%d\n", in.SourceConn.BucketName, rsp.Count, rsp.Size)
	return rsp.Count, rsp.Size, nil
}

func countObjs(ctx context.Context, in *pb.RunJobRequest) (count, size int64, err error) {
	switch in.SourceConn.Type {
	case flowtype.STOR_TYPE_OPENSDS:
		return countOsdsS3Objs(ctx, in)
	default:
		log.Errorf("unsupport storage type:%v\n", in.SourceConn.Type)
	}

	return 0, 0, errors.New(DMERR_UnSupportBackendType)
}

func getOsdsS3Objs(ctx context.Context, in *pb.RunJobRequest, marker string, limit int32) ([]*osdss3.Object, error) {
	log.Debugf("get osds objects begin")

	req := osdss3.ListObjectsRequest{
		Bucket:  in.SourceConn.BucketName,
		Marker:  marker,
		MaxKeys: limit,
	}
	if in.GetFilt() != nil && len(in.Filt.Prefix) > 0 {
		req.Prefix = in.Filt.Prefix
	}
	rsp, err := s3client.ListObjects(ctx, &req)
	if err != nil {
		log.Errorf("list objects failed, bucket=%s, marker=%s, limit=%d, err:%v\n",
			in.SourceConn.BucketName, marker, limit, err)
		return nil, err
	}

	log.Debugf("get osds objects successfully")
	retObjs := make([]*s3.Object, 0)

	for _, list := range rsp.ListOfListOfObjects {
		retObjs = append(retObjs, list.Objects...)
	}
	return retObjs, nil
}

func GetMultipartSize() int64 {
	var size int64 = 16 * 1024 * 1024 // this is the default
	userSetSize, err := strconv.ParseInt(os.Getenv("PARTSIZE"), 10, 64)
	log.Infof("userSetSize=%d, err=%v.\n", userSetSize, err)
	if err == nil {
		//settedSize must be more than 5M and less than 100M
		if userSetSize >= 5 && userSetSize <= 100 {
			size = userSetSize * 1024 * 1024
			log.Infof("Set size to be %d.\n", size)
		}
	}

	return size
}
