package pkg

import (
	"context"

	"github.com/micro/go-log"
	pb "github.com/opensds/go-panda/datamover/proto"
	"github.com/opensds/go-panda/s3/proto"
	"github.com/micro/go-micro/client"
	"os"
	"github.com/opensds/go-panda/datamover/pkg/db"
	"github.com/globalsign/mgo/bson"
	. "github.com/opensds/go-panda/dataflow/pkg/type"
	 "github.com/opensds/go-panda/dataflow/pkg/utils"
	"time"
	"errors"
	"github.com/opensds/go-panda/backend/proto"
	"github.com/aws/aws-sdk-go/aws/credentials"
)

var simuRoutines = 10

type DatamoverService struct{
	s3client s3.S3Service
	bkendclient backend.BackendService
}

type s3Cred struct {
	ak string
	sk string
}

func (myc *s3Cred) Retrieve() (credentials.Value,error){
	cred := credentials.Value{AccessKeyID:myc.ak, SecretAccessKey:myc.sk}
	return cred,nil
}

func (myc *s3Cred) IsExpired() bool {
	return false
}

func NewDatamoverService() pb.DatamoverHandler {
	host := os.Getenv("DB_HOST")
	dbstor := utils.Database{Credential:"unkonwn", Driver:"mongodb", Endpoint:host}
	db.Init(&dbstor)

	return &DatamoverService{
		s3client:s3.NewS3Service("s3", client.DefaultClient),
		bkendclient:backend.NewBackendService("backend", client.DefaultClient),
	}
}

type LocationInfo struct {
	storType string //aws-s3,azure-blob,hw-obs,etc.
	region string
	endPoint string
	access string
	security string
	bakendId string
}

type SourceOject struct {
	storType string //aws-s3,azure-blob,hw-obs,etc.
	objKey string
	backend string
	size int64
}

//format of key:
var locMap map[string]*LocationInfo

func (b *DatamoverService) doMove (ctx context.Context, objs []*SourceOject, capa chan int64, srcLoca *LocationInfo,
	destLoca *LocationInfo) {
	//Only three routines allowed to be running at the same time
	th := make(chan int, simuRoutines)
	for i := 1; i <= len(objs); i++ {
		go b.move(ctx, objs[i], capa, th, srcLoca, destLoca)
		//Create one routine
		th <- 1
		log.Log("  doMigrate: produce 1 routine.")
	}
}

func moveAwsObj(obj *SourceOject, srcLoca *LocationInfo, destLoca *LocationInfo) error {
	/*if obj.backend != srcLoca.bakendId {//for selfdefined connector, obj.backend would be ""
		srcLoca = locMap[obj.backend]
	}

	s3c := s3Cred{ak:srcLoca.access, sk:srcLoca.security}
	creds := credentials.NewCredentials(&s3c)
	sess, err := session.NewSession(&aws.Config{
		Region:aws.String(srcLoca.region),
		Endpoint:aws.String(srcLoca.endPoint),
		Credentials:creds,
	})
	if err != nil {
		log.Logf("New session failed, err:%v\n", err)
		return err
	}
	svc := awss3.New(sess)
	result, err := svc.ListBuckets(nil)*/

	return nil
}

func moveAzureObj(obj *SourceOject, srcLoca *LocationInfo, destLoca *LocationInfo) error {
	return nil
}

func (b *DatamoverService) refreshBackendLocation(ctx context.Context, bkId string) (*LocationInfo,error) {
	//TODO: use read/write lock to synchronize among routines
	loca,exists := locMap[bkId]
	if !exists {
		req := backend.GetBackendRequest{Id:bkId}
		bk,err := b.bkendclient.GetBackend(ctx, &req)
		if err != nil {
			log.Fatalf("Get backend information failed, err:%v\n", err)
			return nil,errors.New("failed")
		} else {
			//TODO:use read/write lock to synchronize among routines
			//TODO: set region to lcation
			locMap[bkId] = &LocationInfo{storType:bk.Backend.Type, endPoint:bk.Backend.Endpoint,
				access:bk.Backend.Access, security:bk.Backend.Security, bakendId:bkId}
			loca = locMap[bkId]
			log.Fatalf("Refresh backend[id=%s,name=%s] successfully.\n", bk.Backend.Id, bk.Backend.Name)
			return loca,nil
		}
	}else {
		return nil,nil
	}
}

func (b *DatamoverService) getConnLocation(ctx context.Context, conn *pb.Connector) (*LocationInfo,error) {
	switch conn.Type {
	case STOR_TYPE_OPENSDS:{
		bkname := conn.GetBucketName()
		reqbk := s3.Bucket{Name:bkname}
		rspbk,err := b.s3client.GetBucket(ctx, &reqbk)
		if err != nil {
			log.Fatalf("Get bucket[%s] information failed when refresh connector location.\n", bkname)
			return nil,errors.New("get bucket information failed")
		}

		return b.refreshBackendLocation(ctx, rspbk.Backend)
	}
	default:{
		log.Fatalf("Unsupport type:%s.\n", conn.Type)
		return nil,errors.New("unsupport type")
	}
	}
}

func (b *DatamoverService) move(ctx context.Context, obj *SourceOject, capa chan int64, th chan int,
	srcLoca *LocationInfo, destLoca *LocationInfo) {
	succeed := true

	//move object
	loca := locMap[obj.backend]
	var err error = nil
	switch loca.storType {
	case STOR_TYPE_AWS_OBJ:
		err = moveAwsObj(obj, srcLoca, destLoca)
	default: //TODO: need to support AWS,Azure,HWS,etc.
		err = errors.New("Not support backend type.")
		log.Fatalf("Not support backend type:%v\n", loca.storType)
	}

	if err != nil {
		succeed = false
	}

	//TODO: what if update meatadata failed
	//update metadata

	if succeed {
		//If migrate success, update capacity
		capa <- obj.size
	}else {
		capa <- 0
	}
	t := <-th
	log.Logf("  migrate: consume %d routine.", t)
}

func (b *DatamoverService) getSourceObjs(ctx context.Context, conn *pb.Connector, filt *pb.Filter,
	defaultSrcLoca *LocationInfo) ([]*SourceOject, error){
	switch conn.Type {
	case STOR_TYPE_OPENSDS:{
		obj := s3.Object{ObjectKey:filt.Prefix, BucketName:conn.BucketName}
		objs,err := b.s3client.ListObjects(ctx, &obj)
		totalObjs := len(objs.ListObjects)
		if err != nil || totalObjs == 0{
			log.Fatalf("List objects failed, err:%v\n", err)
			return nil, err
		}
		srcObjs := []*SourceOject{}
		for i := 0; i < totalObjs; i++ {
			obj := SourceOject{objKey:objs.ListObjects[i].ObjectKey,
				backend:objs.ListObjects[i].Backend, size:objs.ListObjects[i].Size}
			//refresh source location if needed
			if objs.ListObjects[i].Backend != defaultSrcLoca.bakendId {
				//User defined specific backend, which is different from the default backend
				loca,err := b.refreshBackendLocation(ctx, objs.ListObjects[i].Backend)
				if err != nil {
					return nil,err
				}
				obj.storType = loca.storType
			}else {
				obj.storType = defaultSrcLoca.storType
			}
			srcObjs = append(srcObjs, &obj)
		}
		return srcObjs,nil
	}
	default:{
		log.Fatalf("Unsupport storage type:%v\n", conn.Type)
		return nil, errors.New("unsupport storage type")
	}
	}
}


func (b *DatamoverService) Runjob(ctx context.Context, in *pb.RunJobRequest, out *pb.RunJobResponse) error {
	log.Log("Runjob is called in datamover service.")
	log.Logf("Request: %+v\n", in)

	j := Job{Id:bson.ObjectIdHex(in.Id)}
	j.StartTime = time.Now()

    //TODO:Check if source and destination connectors can access.

	//Get source and destination location information
	srcLoca, err := b.getConnLocation(ctx, in.DestConn)
	if err != nil {
		j.Status = JOB_STATUS_FAILED
		j.EndTime = time.Now()
		db.DbAdapter.UpdateJob(&j)
		return err
	}
	destLoca, erro := b.getConnLocation(ctx, in.SourceConn)
	if erro != nil {
		j.Status = JOB_STATUS_FAILED
		j.EndTime = time.Now()
		db.DbAdapter.UpdateJob(&j)
		return erro
	}
	log.Logf("srcLoca=%v, destLoca=%v\n", srcLoca,destLoca)

	//Get Objects which need to be migrated. Calculate the total number and capacity of objects
	objs,err := b.getSourceObjs(ctx, in.SourceConn, in.Filt, srcLoca)
	totalObjs := len(objs)
	if err != nil || totalObjs == 0{
		log.Fatalf("List objects failed, err:%v\n", err)
		//update database
		j.Status = JOB_STATUS_FAILED
		db.DbAdapter.UpdateJob(&j)
		return err
	}
	for i := 0; i < totalObjs; i++ {
		j.TotalCount++
		j.TotalCapacity += objs[i].size
	}
	j.Status = JOB_STATUS_RUNNING
	db.DbAdapter.UpdateJob(&j)

	//Make channel
	capa := make(chan int64)

	//Do migration for each object.
	go b.doMove(ctx, objs, capa, srcLoca, destLoca)

	var capacity int64 = 0
	//TODO: What if a part of objects succeed, but the others failed.
	count := 0
	tmout := true
	for ; ;  {
		select {
		case c := <-capa: {//if c equals 0, that means the object is migrated failed.
			capacity += c
			count += 1
			//TODO:update job in database, need to consider the update frequency
		}
		case <-time.After(86400*time.Second): { //86400 seconds equal one day, we don't provide to migrate to much objects in one plan
			tmout = true
			log.Fatal("Timout.")
		}
		}
		if count >= totalObjs || tmout {
			if tmout {
				out.Err = "timeout"
			}
			log.Logf("break, capacity=%d, timout=%v, count=%v\n", capacity, tmout, count)
			break
		}
	}

	var ret error = nil
	j.PassedCount = count
	if count < totalObjs {
		out.Err = "success"
		ret = errors.New("failed")
		j.Status = JOB_STATUS_FAILED
	}else {
		out.Err = "success"
		j.Status = JOB_STATUS_SUCCEED
	}

	db.DbAdapter.UpdateJob(&j)
	return ret
}
