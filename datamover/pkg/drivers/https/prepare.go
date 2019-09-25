package migration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	flowtype "github.com/opensds/multi-cloud/dataflow/pkg/model"
	flowutils "github.com/opensds/multi-cloud/dataflow/pkg/utils"
	"github.com/opensds/multi-cloud/datamover/pkg/amazon/s3"
	"github.com/opensds/multi-cloud/datamover/pkg/azure/blob"
	"github.com/opensds/multi-cloud/datamover/pkg/ceph/s3"
	"github.com/opensds/multi-cloud/datamover/pkg/db"
	"github.com/opensds/multi-cloud/datamover/pkg/gcp/s3"
	"github.com/opensds/multi-cloud/datamover/pkg/huawei/obs"
	"github.com/opensds/multi-cloud/datamover/pkg/ibm/cos"
	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
	pb "github.com/opensds/multi-cloud/datamover/proto"
	osdss3 "github.com/opensds/multi-cloud/s3/proto"
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

func refreshSrcLocation(ctx context.Context, obj *osdss3.Object, srcLoca *LocationInfo, destLoca *LocationInfo,
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
		reqbk := osdss3.BaseRequest{Id: virtBkname}
		rspbk, err := s3client.GetBucket(ctx, &reqbk)
		if err != nil {
			logger.Printf("get bucket[%s] information failed when refresh connector location, err:%v\n", virtBkname, err)
			return nil, errors.New("get bucket information failed")
		}
		return getOsdsLocation(ctx, virtBkname, rspbk.DefaultLocation)
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

func getObjs(ctx context.Context, in *pb.RunJobRequest, defaultSrcLoca *LocationInfo, offset, limit int32) ([]*osdss3.Object, error) {
	switch in.SourceConn.Type {
	case flowtype.STOR_TYPE_OPENSDS:
		return getOsdsS3Objs(ctx, in, offset, limit)
	default:
		logger.Printf("unsupport storage type:%v\n", in.SourceConn.Type)
	}

	return nil, errors.New(DMERR_InternalError)
}

func countOsdsS3Objs(ctx context.Context, in *pb.RunJobRequest) (count, size int64, err error) {
	logger.Printf("count objects of bucket[%s]\n", in.SourceConn.BucketName)
	filt := make(map[string]string)
	if in.GetFilt() != nil && len(in.Filt.Prefix) > 0 {
		filt[flowutils.KObjKey] = "^" + in.Filt.Prefix
	}

	req := osdss3.ListObjectsRequest{
		Bucket: in.SourceConn.BucketName,
		Filter: filt,
	}
	rsp, err := s3client.CountObjects(ctx, &req)
	if err != nil {
		return 0, 0, errors.New(DMERR_InternalError)
	}

	logger.Printf("count objects of bucket[%s]: count=%d,size=%d\n", in.SourceConn.BucketName, count, size)
	return rsp.Count, rsp.Size, nil
}

func countObjs(ctx context.Context, in *pb.RunJobRequest) (count, size int64, err error) {
	switch in.SourceConn.Type {
	case flowtype.STOR_TYPE_OPENSDS:
		return countOsdsS3Objs(ctx, in)
	default:
		logger.Printf("unsupport storage type:%v\n", in.SourceConn.Type)
	}

	return 0, 0, errors.New(DMERR_UnSupportBackendType)
}

func getOsdsS3Objs(ctx context.Context, in *pb.RunJobRequest, offset, limit int32) ([]*osdss3.Object, error) {
	logger.Println("get osds objects begin")
	filt := make(map[string]string)
	if in.GetFilt() != nil && len(in.Filt.Prefix) > 0 {
		filt[flowutils.KObjKey] = "^" + in.Filt.Prefix
	}

	req := osdss3.ListObjectsRequest{
		Bucket: in.SourceConn.BucketName,
		Filter: filt,
		Offset: offset,
		Limit:  limit,
	}
	rsp, err := s3client.ListObjects(ctx, &req)
	if err != nil {
		logger.Printf("list objects failed, bucket=%s, offset=%d, limit=%d, err:%v\n", in.SourceConn.BucketName, offset, limit, err)
		return nil, err
	}

	logger.Println("get osds objects successfully")
	return rsp.ListObjects, nil
}

func getIBMCosObjs(ctx context.Context, conn *pb.Connector, filt *pb.Filter,
	defaultSrcLoca *LocationInfo) ([]*osdss3.Object, error) {
	//TODO(acorbellini): reuse getAWSS3Objs function
	srcObjs := []*osdss3.Object{}
	objs, err := ibmcosmover.ListObjs(defaultSrcLoca, filt)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(objs); i++ {
		obj := osdss3.Object{Size: *objs[i].Size, ObjectKey: *objs[i].Key, Location: ""}
		srcObjs = append(srcObjs, &obj)
	}
	return srcObjs, nil
}

func getAwsS3Objs(ctx context.Context, conn *pb.Connector, filt *pb.Filter,
	defaultSrcLoca *LocationInfo) ([]*osdss3.Object, error) {
	//TODO:need to support filter
	srcObjs := []*osdss3.Object{}
	objs, err := s3mover.ListObjs(defaultSrcLoca, filt)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(objs); i++ {
		obj := osdss3.Object{Size: *objs[i].Size, ObjectKey: *objs[i].Key, Location: ""}
		srcObjs = append(srcObjs, &obj)
	}
	return srcObjs, nil
}

func getHwObjs(ctx context.Context, conn *pb.Connector, filt *pb.Filter,
	defaultSrcLoca *LocationInfo) ([]*osdss3.Object, error) {
	//TODO:need to support filter
	srcObjs := []*osdss3.Object{}
	objs, err := obsmover.ListObjs(defaultSrcLoca, filt)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(objs); i++ {
		obj := osdss3.Object{Size: objs[i].Size, ObjectKey: objs[i].Key, Location: ""}
		srcObjs = append(srcObjs, &obj)
	}
	return srcObjs, nil
}

func getAzureBlobs(ctx context.Context, conn *pb.Connector, filt *pb.Filter,
	defaultSrcLoca *LocationInfo) ([]*osdss3.Object, error) {
	srcObjs := []*osdss3.Object{}
	objs, err := blobmover.ListObjs(defaultSrcLoca, filt)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(objs); i++ {
		obj := osdss3.Object{Size: *objs[i].Properties.ContentLength, ObjectKey: objs[i].Name,
		Location: ""}
		srcObjs = append(srcObjs, &obj)
	}
	return srcObjs, nil
}

//to get object details from ceph backend
func getCephS3Objs(ctx context.Context, conn *pb.Connector, filt *pb.Filter,
	defaultSrcLoca *LocationInfo) ([]*osdss3.Object, error) {
	srcObjs := []*osdss3.Object{}
	objs, err := cephs3mover.ListObjs(defaultSrcLoca, filt)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(objs); i++ {
		obj := osdss3.Object{Size: objs[i].Size, ObjectKey: objs[i].Key, Location: ""}
		srcObjs = append(srcObjs, &obj)
	}
	return srcObjs, nil
}

//to get object details from gcp backend
func getGcpS3Objs(ctx context.Context, conn *pb.Connector, filt *pb.Filter,
	defaultSrcLoca *LocationInfo) ([]*osdss3.Object, error) {
	srcObjs := []*osdss3.Object{}
	objs, err := Gcps3mover.ListObjs(defaultSrcLoca, filt)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(objs); i++ {
		obj := osdss3.Object{Size: objs[i].Size, ObjectKey: objs[i].Key, Location: ""}
		srcObjs = append(srcObjs, &obj)
	}
	return srcObjs, nil
}
