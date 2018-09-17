package pkg

import (
	"context"

	"github.com/micro/go-log"
	pb "github.com/opensds/go-panda/datamover/proto"
	osdss3 "github.com/opensds/go-panda/s3/proto"
	"github.com/micro/go-micro/client"
	"os"
	"github.com/opensds/go-panda/datamover/pkg/db"
	flowtype "github.com/opensds/go-panda/dataflow/pkg/type"
	 "github.com/opensds/go-panda/dataflow/pkg/utils"
	"errors"
	"github.com/opensds/go-panda/backend/proto"
	"time"
	"github.com/globalsign/mgo/bson"
	. "github.com/opensds/go-panda/datamover/pkg/utils"
	"github.com/opensds/go-panda/datamover/pkg/hw/obs"
	"github.com/opensds/go-panda/datamover/pkg/amazon/s3"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
)

var simuRoutines = 10
var ObjSizeLimit int64 = 50 * 1024 * 1024 //The max object size that can be moved directly
var PART_SIZE int64 = 5 * 1024 * 1024
type DatamoverService struct{
	s3client osdss3.S3Service
	bkendclient backend.BackendService
}

func NewDatamoverService() pb.DatamoverHandler {
	host := os.Getenv("DB_HOST")
	dbstor := utils.Database{Credential:"unkonwn", Driver:"mongodb", Endpoint:host}
	db.Init(&dbstor)

	return &DatamoverService{
		s3client:osdss3.NewS3Service("s3", client.DefaultClient),
		bkendclient:backend.NewBackendService("backend", client.DefaultClient),
	}
}

//format of key:
var locMap map[string]*LocationInfo

func (b *DatamoverService) doMove (ctx context.Context, objs []*SourceOject, capa chan int64, srcLoca *LocationInfo,
	destLoca *LocationInfo, remainSource bool) {
	//Only three routines allowed to be running at the same time
	th := make(chan int, simuRoutines)
	for i := 1; i <= len(objs); i++ {
		go b.move(ctx, objs[i], capa, th, srcLoca, destLoca, remainSource)
		//Create one routine
		th <- 1
		log.Log("  doMigrate: produce 1 routine.")
	}
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
			locMap[bkId] = &LocationInfo{bk.Backend.Type, bk.Backend.Endpoint, bk.Backend.Endpoint,
			bk.Backend.BucketName, bk.Backend.Access, bk.Backend.Security, bkId}
			//locMap[bkId].region = ""
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
	case flowtype.STOR_TYPE_OPENSDS:{
		bkname := conn.GetBucketName()
		reqbk := osdss3.Bucket{Name:bkname}
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

func moveObj(obj *SourceOject, srcLoca *LocationInfo, destLoca *LocationInfo) error {
	buf := make([]byte, obj.Size)

	var size int64 = 0
	var err error = nil

	//download
	switch  srcLoca.StorType {
	case flowtype.STOR_TYPE_HW_OBS:{
		size, err = obsmover.DownloadHwObsObj(obj, srcLoca, buf)
	}
	case flowtype.STOR_TYPE_AWS_S3:{
		size, err = s3mover.DownloadS3Obj(obj, srcLoca, buf)
	}
	default:{
		log.Fatalf("Not support source backend type:%v\n", srcLoca.StorType)
		err = errors.New("Not support source backend type.")
	}
	}

	if err != nil {
		log.Fatal("Download object failed.")
		return err
	}
	log.Logf("Download object succeed, size=%d\n", size)

	//upload
	switch destLoca.StorType {
	case flowtype.STOR_TYPE_HW_OBS:{
		err = obsmover.UploadHwObsObj(obj, destLoca, buf)
	}
	case flowtype.STOR_TYPE_AWS_S3:{
		err = s3mover.UploadS3Obj(obj, destLoca, buf)
	}
	default:
		log.Fatalf("Not support destination backend type:%v\n", destLoca.StorType)
		return errors.New("Not support destination backend type.")
	}

	return nil
}

func downloadRange(obj *SourceOject, srcLoca *LocationInfo, buf []byte, start int64, end int64) (size int64, err error) {
	switch srcLoca.StorType {
	case flowtype.STOR_TYPE_AWS_S3:
	case flowtype.STOR_TYPE_HW_OBS:
		return s3mover.DownloadRangeS3(obj.ObjKey, srcLoca, buf, start, end)
	default:
		log.Logf("Unsupport storType[%d] to download.\n", srcLoca.StorType)
	}

	return 0,errors.New("Unsupport storage type.")
}

func multiPartUploadInit(obj *SourceOject, destLoca *LocationInfo) (svc *s3.S3, uploadId string, err error) {
	switch destLoca.StorType {
	case flowtype.STOR_TYPE_AWS_S3:
	case flowtype.STOR_TYPE_HW_OBS:
		return s3mover.MultiPartUploadInitS3(obj.ObjKey, destLoca)
	default:
		log.Logf("Unsupport storType[%d] to download.\n", destLoca.StorType)
	}

	return nil,"",errors.New("Unsupport storage type.")
}

func uploadPartS3(obj *SourceOject, destLoca *LocationInfo, svc *s3.S3, uploadId string, upBytes int64, buf []byte, partNumber int64)(*s3.CompletedPart, error) {
	switch destLoca.StorType {
	case flowtype.STOR_TYPE_AWS_S3:
	case flowtype.STOR_TYPE_HW_OBS:
		return s3mover.UploadPartS3(obj.ObjKey, destLoca, svc, uploadId, upBytes, buf, partNumber)
	default:
		log.Logf("Unsupport storType[%d] to download.\n", destLoca.StorType)
	}

	return nil,errors.New("Unsupport storage type.")
}

func abortMultipartUpload(objKey string, destLoca *LocationInfo, svc *s3.S3, uploadId string) error {
	switch destLoca.StorType {
	case flowtype.STOR_TYPE_AWS_S3:
	case flowtype.STOR_TYPE_HW_OBS:
		return s3mover.AbortMultipartUpload(objKey, destLoca, svc, uploadId)
	default:
		log.Logf("Unsupport storType[%d] to download.\n", destLoca.StorType)
	}

	return errors.New("Unsupport storage type.")
}

func completeMultipartUpload(objKey string, destLoca *LocationInfo, svc *s3.S3, uploadId string, completeParts []*s3.CompletedPart) error {
	switch destLoca.StorType {
	case flowtype.STOR_TYPE_AWS_S3:
	case flowtype.STOR_TYPE_HW_OBS:
		return s3mover.CompleteMultipartUploadS3(objKey, destLoca, svc, uploadId, completeParts)
	default:
		log.Logf("Unsupport storType[%d] to download.\n", destLoca.StorType)
	}

	return errors.New("Unsupport storage type.")
}

func multipartMoveObj(obj *SourceOject, srcLoca *LocationInfo, destLoca *LocationInfo) error {
	partCount := int64(obj.Size / PART_SIZE)
	if obj.Size%PART_SIZE != 0 {
		partCount++
	}

	buf := make([]byte, PART_SIZE)
	var i int64
	var uploadId string = ""
	var svc *s3.S3
	var completeParts []*s3.CompletedPart
	for i = 0; i < partCount; i++ {
		partNumber := i + 1
		offset := int64(i) * PART_SIZE
		currPartSize := PART_SIZE
		if i+1 == partCount {
			currPartSize = obj.Size - offset
			buf = nil
			buf = make([]byte, currPartSize)
		}

		start := offset
		end := offset + currPartSize - 1
		readSize, err := downloadRange(obj, srcLoca, buf, start, end)
		//err := downloadPartS3(partNumber, buf, start, end)
		if err != nil {
			return errors.New("Download failed.")
		}
		//fmt.Printf("Download part %d range[%d:%d] successfully.\n", partNumber, offset, end)
		if int64(readSize) != currPartSize {
			fmt.Printf("Internal error, currPartSize=%d, readSize=%d\n", currPartSize, readSize)
			return errors.New("Internal error")
		}

		if partNumber == 1 {
			//init multipart upload
			svc, uploadId, err = multiPartUploadInit(obj, destLoca)
			if err != nil {
				return err
			}
		}

		completePart, err1 := uploadPartS3(obj, destLoca, svc, uploadId, currPartSize, buf, partNumber)
		if err1 != nil {
			err := abortMultipartUpload(obj.ObjKey, destLoca, svc, uploadId)
			if err != nil {
				fmt.Printf("Abort s3 multipart upload failed, err:%v\n", err)
			}
			return errors.New("S3 multipart upload failed.")
		}
		completeParts = append(completeParts, completePart)
	}

	err := completeMultipartUpload(obj.ObjKey, destLoca, svc, uploadId, completeParts)
	if err != nil {
		fmt.Println(err.Error())
	}else {
		fmt.Println("Move successfully.")
	}

	return err
	return nil
}

func (b *DatamoverService) move(ctx context.Context, obj *SourceOject, capa chan int64, th chan int,
	srcLoca *LocationInfo, destLoca *LocationInfo, remainSource bool) {
	succeed := true
	if obj.Backend != srcLoca.BakendId {//for selfdefined connector, obj.backend and srcLoca.bakendId would be ""
		//TODO: use read/wirte lock
		srcLoca = locMap[obj.Backend]
	}

	//move object
	var err error
	if obj.Size < ObjSizeLimit {
		err = moveObj(obj, srcLoca, destLoca)
	}else {
		err = multipartMoveObj(obj, srcLoca, destLoca)
	}

	if err != nil {
		succeed = false
	}

	//TODO: what if update meatadata failed
	//update metadata
	if succeed {

	}

	//Delete source data if needed
	//TODO: what if delete failed
	if !remainSource {

	}


	if succeed {
		//If migrate success, update capacity
		capa <- obj.Size
	}else {
		capa <- 0
	}
	t := <-th
	log.Logf("  migrate: consume %d routine.", t)
}

func (b *DatamoverService) getSourceObjs(ctx context.Context, conn *pb.Connector, filt *pb.Filter,
	defaultSrcLoca *LocationInfo) ([]*SourceOject, error){
	switch conn.Type {
	case flowtype.STOR_TYPE_OPENSDS:{
		obj := osdss3.Object{ObjectKey:filt.Prefix, BucketName:conn.BucketName}
		objs,err := b.s3client.ListObjects(ctx, &obj)
		totalObjs := len(objs.ListObjects)
		if err != nil || totalObjs == 0{
			log.Fatalf("List objects failed, err:%v\n", err)
			return nil, err
		}
		srcObjs := []*SourceOject{}
		for i := 0; i < totalObjs; i++ {
			obj := SourceOject{ObjKey:objs.ListObjects[i].ObjectKey,
				Backend:objs.ListObjects[i].Backend, Size:objs.ListObjects[i].Size}
			//refresh source location if needed
			if objs.ListObjects[i].Backend != defaultSrcLoca.BakendId {
				//User defined specific backend, which is different from the default backend
				loca,err := b.refreshBackendLocation(ctx, objs.ListObjects[i].Backend)
				if err != nil {
					return nil,err
				}
				obj.StorType = loca.StorType
			}else {
				obj.StorType = defaultSrcLoca.StorType
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

	j := flowtype.Job{Id:bson.ObjectIdHex(in.Id)}
	j.StartTime = time.Now()

    //TODO:Check if source and destination connectors can access.

	//Get source and destination location information
	srcLoca, err := b.getConnLocation(ctx, in.DestConn)
	if err != nil {
		j.Status = flowtype.JOB_STATUS_FAILED
		j.EndTime = time.Now()
		db.DbAdapter.UpdateJob(&j)
		return err
	}
	destLoca, erro := b.getConnLocation(ctx, in.SourceConn)
	if erro != nil {
		j.Status = flowtype.JOB_STATUS_FAILED
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
		j.Status = flowtype.JOB_STATUS_FAILED
		db.DbAdapter.UpdateJob(&j)
		return err
	}
	for i := 0; i < totalObjs; i++ {
		j.TotalCount++
		j.TotalCapacity += objs[i].Size
	}
	j.Status = flowtype.JOB_STATUS_RUNNING
	db.DbAdapter.UpdateJob(&j)

	//Make channel
	capa := make(chan int64)

	//Do migration for each object.
	go b.doMove(ctx, objs, capa, srcLoca, destLoca, in.RemainSource)

	var capacity int64 = 0
	//TODO: What if a part of objects succeed, but the others failed.
	count := 0
	passedCount := 0
	//failedCount := 0
	tmout := true
	for ; ;  {
		select {
		case c := <-capa: {//if c equals 0, that means the object is migrated failed.
			capacity += c
			count++
			if capacity != 0 {
				passedCount++
			}
			//TODO:update job in database, need to consider the update frequency
			var deci int = totalObjs/10
			if totalObjs < 100 || count == totalObjs || count%deci == 0 {
				//update database
				j.PassedCount = passedCount
				j.PassedCapacity = capacity
				db.DbAdapter.UpdateJob(&j)
			}
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
	j.PassedCount = passedCount
	if passedCount < totalObjs {
		out.Err = "success"
		ret = errors.New("failed")
		j.Status = flowtype.JOB_STATUS_FAILED
	}else {
		out.Err = "success"
		j.Status = flowtype.JOB_STATUS_SUCCEED
	}

	for i := 1; i <= 3; i++ {
		err := db.DbAdapter.UpdateJob(&j)
		if err == nil {
			break
		}
		if i == 3 {
			log.Fatalf("Update the finish status of job in database failed three times, no need to try more.")
		}
	}

	return ret
}
