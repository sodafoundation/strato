package migration

import (
	"context"
	"errors"
	"fmt"
	s3 "github.com/opensds/multi-cloud/s3/proto"
	"os"
	"strconv"
	"time"

	flowtype "github.com/opensds/multi-cloud/dataflow/pkg/model"
	"github.com/opensds/multi-cloud/datamover/pkg/db"
	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
	pb "github.com/opensds/multi-cloud/datamover/proto"
	log "github.com/sirupsen/logrus"
)

func getCtxTimeout() time.Duration {
	// 1 day as default
	tmout := 86400 * time.Second

	tmoutCfg, err := strconv.ParseInt(os.Getenv("JOB_MAX_RUN_TIME"), 10, 64)
	if err != nil || tmoutCfg < 60 || tmoutCfg > 2592000 {
		tmoutCfg = int64(JOB_RUN_TIME_MAX)
	}
	durStr := fmt.Sprintf("%ds", tmoutCfg)
	logger.Printf("Vaule of JOB_MAX_RUN_TIME is: %d seconds, durStr:%s.\n", tmoutCfg, durStr)
	val, err := time.ParseDuration(durStr)
	if err == nil {
		tmout = val
	}

	logger.Printf("tmout=%v\n", tmout)
	return tmout
}

func getLocationInfo(ctx context.Context, j *flowtype.Job, in *pb.RunJobRequest) (srcLoca *LocationInfo, destLoca *LocationInfo, err error) {
	srcLoca, err = getConnLocation(ctx, in.SourceConn)
	if err != nil {
		logger.Printf("err:%v\n", err)
		return nil, nil, err
	}
	logger.Printf("srcLoca:StorType=%s,VirBucket=%s,BucketName=%s,Region=%s\n",
		srcLoca.StorType, srcLoca.VirBucket, srcLoca.BucketName, srcLoca.Region)
	destLoca, err = getConnLocation(ctx, in.DestConn)
	if err != nil {
		db.DbAdapter.UpdateJob(j)
		return nil, nil, err
	}
	logger.Printf("destLoca:srcLoca:StorType=%s,VirBucket=%s,BucketName=%s,Region=%s\n",
		destLoca.StorType, destLoca.VirBucket, destLoca.BucketName, destLoca.Region)
	logger.Println("Get location information successfully.")

	return srcLoca, destLoca, err
}

func refreshSrcLocation(ctx context.Context, obj *s3.Object, srcLoca *LocationInfo, destLoca *LocationInfo,
	locMap map[string]*LocationInfo) (newSrcLoca *LocationInfo, err error) {
	if obj.Location != srcLoca.BakendName && obj.Location != "" {
		//If oject does not use the default backend
		logger.Printf("locaMap:%+v\n", locMap)
		//for selfdefined connector, obj.Location and srcLoca.backendname would be ""
		//TODO: use read/wirte lock
		newLoc, exists := locMap[obj.Location]
		if !exists {
			newLoc, err = getOsdsLocation(ctx, obj.BucketName, obj.Location)
			if err != nil {
				return nil, err
			}
		}
		locMap[obj.Location] = newLoc
		logger.Printf("newSrcLoca=%+v\n", newLoc)
		return newLoc, nil
	}

	return srcLoca, nil
}

func getOsdsLocation(ctx context.Context, virtBkname string, backendName string) (*LocationInfo, error) {
	if backendName == "" {
		logger.Println("get backend location failed, because backend name is null.")
		return nil, errors.New("failed")
	}

	bk, err := db.DbAdapter.GetBackendByName(backendName)
	if err != nil {
		logger.Printf("get backend information failed, err:%v\n", err)
		return nil, errors.New("failed")
	} else {
		loca := &LocationInfo{StorType: bk.Type, Region: bk.Region, EndPoint: bk.Endpoint, BucketName: bk.BucketName,
			VirBucket: virtBkname, Access: bk.Access, Security: bk.Security, BakendName: backendName}
		logger.Printf("Refresh backend[name:%s,id:%s] successfully.\n", backendName, bk.Id.String())
		return loca, nil
	}
}

func getConnLocation(ctx context.Context, conn *pb.Connector) (*LocationInfo, error) {
	switch conn.Type {
	case flowtype.STOR_TYPE_OPENSDS:
		virtBkname := conn.GetBucketName()
		rspbk, err := s3client.GetBucket(ctx, &s3.Bucket{Name: virtBkname})
		if err != nil {
			logger.Printf("get bucket[%s] information failed when refresh connector location, err:%v\n", virtBkname, err)
			return nil, errors.New("get bucket information failed")
		}
		return getOsdsLocation(ctx, virtBkname, rspbk.BucketMeta.DefaultLocation)
	case flowtype.STOR_TYPE_AWS_S3, flowtype.STOR_TYPE_HW_OBS, flowtype.STOR_TYPE_HW_FUSIONSTORAGE, flowtype.STOR_TYPE_AZURE_BLOB, flowtype.STOR_TYPE_CEPH_S3, flowtype.STOR_TYPE_GCP_S3, flowtype.STOR_TYPE_IBM_COS:
		cfg := conn.ConnConfig
		loca := LocationInfo{}
		loca.StorType = conn.Type
		for i := 0; i < len(cfg); i++ {
			switch cfg[i].Key {
			case "region":
				loca.Region = cfg[i].Value
			case "endpoint":
				loca.EndPoint = cfg[i].Value
			case "bucketname":
				loca.BucketName = cfg[i].Value
			case "access":
				loca.Access = cfg[i].Value
			case "security":
				loca.Security = cfg[i].Value
			default:
				logger.Printf("unknow key[%s] for connector.\n", cfg[i].Key)
			}
		}
		return &loca, nil
	default:
		logger.Printf("unsupport type:%s.\n", conn.Type)
	}

	return nil, errors.New("unsupport type")
}

func getObjs(ctx context.Context, in *pb.RunJobRequest, marker string, limit int32) ([]*s3.Object, error) {
	switch in.SourceConn.Type {
	case flowtype.STOR_TYPE_OPENSDS:
		return gets3Objs(ctx, in, marker, limit)
	default:
		logger.Printf("unsupport storage type:%v\n", in.SourceConn.Type)
	}

	return nil, errors.New(DMERR_InternalError)
}

func counts3Objs(ctx context.Context, in *pb.RunJobRequest) (count, size int64, err error) {
	logger.Printf("count objects of bucket[%s].\n", in.SourceConn.BucketName)

	req := s3.ListObjectsRequest{Bucket: in.SourceConn.BucketName}
	if in.GetFilt() != nil && len(in.Filt.Prefix) > 0 {
		req.Prefix = in.Filt.Prefix
	}

	rsp, err := s3client.CountObjects(ctx, &req)
	if err != nil {
		logger.Printf("err: %v\n", err)
		return 0, 0, errors.New(DMERR_InternalError)
	}

	logger.Printf("count objects of bucket[%s]: count=%d,size=%d\n", in.SourceConn.BucketName, rsp.Count, rsp.Size)
	return rsp.Count, rsp.Size, nil
}

func countObjs(ctx context.Context, in *pb.RunJobRequest) (count, size int64, err error) {
	switch in.SourceConn.Type {
	case flowtype.STOR_TYPE_OPENSDS:
		return counts3Objs(ctx, in)
	default:
		logger.Printf("unsupport storage type:%v\n", in.SourceConn.Type)
	}

	return 0, 0, errors.New(DMERR_UnSupportBackendType)
}

func gets3Objs(ctx context.Context, in *pb.RunJobRequest, marker string, limit int32) ([]*s3.Object, error) {
	logger.Println("get osds objects begin")

	req := s3.ListObjectsRequest{
		Bucket:  in.SourceConn.BucketName,
		Marker:  marker,
		MaxKeys: limit,
	}
	if in.GetFilt() != nil && len(in.Filt.Prefix) > 0 {
		req.Prefix = in.Filt.Prefix
	}
	rsp, err := s3client.ListObjects(ctx, &req)
	if err != nil {
		logger.Printf("list objects failed, bucket=%s, marker=%s, limit=%d, err:%v\n",
			in.SourceConn.BucketName, marker, limit, err)
		return nil, err
	}

	logger.Println("get osds objects successfully")
	retObjArr := make([]*s3.Object,0)
	for _,objArrPtr := range rsp.ListOfListOfObjects{
		for _, obj := range objArrPtr.Objects{
			retObjArr = append(retObjArr, obj)
		}
	}
	return retObjArr, nil
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
