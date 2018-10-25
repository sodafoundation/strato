package migration

import (
	"strconv"
	"time"
	"github.com/opensds/multi-cloud/datamover/pkg/hw/obs"
	"github.com/opensds/multi-cloud/datamover/pkg/amazon/s3"
	"github.com/opensds/multi-cloud/datamover/pkg/azure/blob"
	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
	"github.com/opensds/multi-cloud/backend/proto"
	pb "github.com/opensds/multi-cloud/datamover/proto"
	osdss3 "github.com/opensds/multi-cloud/s3/proto"
	flowtype "github.com/opensds/multi-cloud/dataflow/pkg/model"
	"github.com/opensds/multi-cloud/datamover/pkg/db"
	"github.com/globalsign/mgo/bson"
	"errors"
	"context"
	"encoding/json"
	"github.com/micro/go-micro/client"
	"os"
	"log"
)

var simuRoutines = 10
var ObjSizeLimit int64 = 50 * 1024 * 1024 //The max object size that can be moved directly
var PART_SIZE int64 = 50 * 1024 * 1024
//format of key:
var locMap map[string]*LocationInfo
var s3client osdss3.S3Service
var bkendclient backend.BackendService

var logger = log.New(os.Stdout, "", log.LstdFlags)

type Migration interface {
	Init()
	HandleMsg(msg string)
}

func Init() {
	logger.Println("Migration init.")
	locMap = make(map[string]*LocationInfo)
	s3client = osdss3.NewS3Service("s3", client.DefaultClient)
	bkendclient = backend.NewBackendService("backend", client.DefaultClient)
}

func HandleMsg(msgData []byte) error {
	var job pb.RunJobRequest
	err := json.Unmarshal(msgData, &job)
	if err != nil {
		logger.Printf("Unmarshal failed, err:%v\n", err)
		return err
	}

	//Check the status of job, and run it if needed
	status := db.DbAdapter.GetJobStatus(job.Id)
	if status != flowtype.JOB_STATUS_PENDING {
		logger.Printf("Job[ID#%s] is not in %s status.\n", job.Id, flowtype.JOB_STATUS_PENDING)
		return errors.New("Job already running.")
	}

	logger.Printf("HandleMsg:job=%v\n",job)
	go runjob(&job)
	return nil
}

func doMove (ctx context.Context, objs []*SourceOject, capa chan int64, th chan int, srcLoca *LocationInfo,
	destLoca *LocationInfo, remainSource bool) {
	//Only three routines allowed to be running at the same time
	//th := make(chan int, simuRoutines)
	for i := 0; i < len(objs); i++ {
		logger.Printf("Begin to move obj(key:%s)\n", objs[i].Obj.ObjectKey)
		go move(ctx, objs[i], capa, th, srcLoca, destLoca, remainSource)
		//Create one routine
		th <- 1
		logger.Println("doMigrate: produce 1 routine.")
	}
}

func refreshBackendLocation(ctx context.Context, virtBkname string, bkId string) (*LocationInfo,error) {
	//TODO: use read/write lock to synchronize among routines
	if bkId == "" {
		logger.Println("Get backend location failed, because backend id is null.")
		return nil,errors.New("failed")
	}
	loca,exists := locMap[bkId]
	if !exists {
		logger.Printf("Backend(id:%s) is not in the map, need to build it.\n", bkId)
		req := backend.GetBackendRequest{Id:bkId}
		bk,err := bkendclient.GetBackend(ctx, &req)
		if err != nil {
			logger.Printf("Get backend information failed, err:%v\n", err)
			return nil,errors.New("failed")
		} else {
			//TODO:use read/write lock to synchronize among routines
			loca = &LocationInfo{bk.Backend.Type, bk.Backend.Region, bk.Backend.Endpoint,
				bk.Backend.BucketName, virtBkname,bk.Backend.Access, bk.Backend.Security, bkId}
			//locMap[bkId].region = ""
			//loca = locMap[bkId]
			locMap[bkId] = loca
			logger.Printf("Refresh backend[id:%s,name:%s] successfully.\n", bkId, bk.Backend.BucketName)
			return loca,nil
		}
	}else {
		logger.Printf("Backend(id:%s) is in the map:%+v\n", loca)
		return loca,nil
	}
}

func getConnLocation(ctx context.Context, conn *pb.Connector) (*LocationInfo,error) {
	switch conn.Type {
	case flowtype.STOR_TYPE_OPENSDS:{
		virtBkname := conn.GetBucketName()
		reqbk := osdss3.Bucket{Name:virtBkname}
		rspbk,err := s3client.GetBucket(ctx, &reqbk)
		if err != nil {
			logger.Printf("Get bucket[%s] information failed when refresh connector location, err:%v\n", virtBkname, err)
			return nil,errors.New("get bucket information failed")
		}

		return refreshBackendLocation(ctx, virtBkname, rspbk.Backend)
	}
	case flowtype.STOR_TYPE_AWS_S3, flowtype.STOR_TYPE_HW_OBS, flowtype.STOR_TYPE_HW_FUSIONSTORAGE, flowtype.STOR_TYPE_AZURE_BLOB:{
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
				logger.Printf("Uknow key[%s] for connector.\n", cfg[i].Key)
			}
		}
		return &loca, nil
	}
	default:{
		logger.Printf("Unsupport type:%s.\n", conn.Type)
		return nil,errors.New("unsupport type")
	}
	}
}

func moveObj(obj *SourceOject, srcLoca *LocationInfo, destLoca *LocationInfo) error {
	//buf := make([]byte, 2579)
	logger.Printf("Obj[%s] size is %d.\n", obj.Obj.ObjectKey, obj.Obj.Size)
	if obj.Obj.Size <= 0 {
		return nil
	}
	buf := make([]byte, obj.Obj.Size)

	var size int64 = 0
	var err error = nil

	var downloader,uploader MoveWorker
	downloadObjKey := obj.Obj.ObjectKey
	if srcLoca.VirBucket != "" {
		downloadObjKey = srcLoca.VirBucket + "/" + downloadObjKey
	}
	//download
	switch  srcLoca.StorType {
	case flowtype.STOR_TYPE_HW_OBS, flowtype.STOR_TYPE_HW_FUSIONSTORAGE, flowtype.STOR_TYPE_HW_FUSIONCLOUD:
		downloader = &obsmover.ObsMover{}
		size, err = downloader.DownloadObj(downloadObjKey, srcLoca, buf)
	case flowtype.STOR_TYPE_AWS_S3:
		downloader = &s3mover.S3Mover{}
		size, err = downloader.DownloadObj(downloadObjKey, srcLoca, buf)
	case flowtype.STOR_TYPE_AZURE_BLOB:
		downloader = &blobmover.BlobMover{}
		size, err = downloader.DownloadObj(downloadObjKey, srcLoca, buf)
	default:{
		logger.Printf("Not support source backend type:%v\n", srcLoca.StorType)
		err = errors.New("Not support source backend type.")
	}
	}

	if err != nil {
		logger.Printf("Download object failed.")
		return err
	}
	logger.Printf("Download object succeed, size=%d\n", size)
	logger.Printf("buf.len:%d\n", len(buf))

	//upload
	uploadObjKey := obj.Obj.ObjectKey
	if srcLoca.VirBucket != "" {
		uploadObjKey = destLoca.VirBucket + "/" + uploadObjKey
	}

	switch destLoca.StorType {
	case flowtype.STOR_TYPE_HW_OBS, flowtype.STOR_TYPE_HW_FUSIONSTORAGE, flowtype.STOR_TYPE_HW_FUSIONCLOUD:
		uploader = &obsmover.ObsMover{}
		err = uploader.UploadObj(uploadObjKey, destLoca, buf)
	case flowtype.STOR_TYPE_AWS_S3:
		uploader = &s3mover.S3Mover{}
		err = uploader.UploadObj(uploadObjKey, destLoca, buf)
	case flowtype.STOR_TYPE_AZURE_BLOB:
		uploader = &blobmover.BlobMover{}
		err = uploader.UploadObj(uploadObjKey, destLoca, buf)
	default:
		logger.Printf("Not support destination backend type:%v\n", destLoca.StorType)
		return errors.New("Not support destination backend type.")
	}
	if err != nil {
		logger.Printf("Upload object[bucket:%s,key:%s] failed, err:%v.\n", destLoca.BucketName, uploadObjKey, err)
	}else {
		logger.Printf("Upload object[bucket:%s,key:%s] succeed.\n", destLoca.BucketName, uploadObjKey)
	}

	return nil
}


func multiPartDownloadInit(srcLoca *LocationInfo) (mover MoveWorker, err error) {
	switch srcLoca.StorType {
	case flowtype.STOR_TYPE_AWS_S3:
		mover := &s3mover.S3Mover{}
		err := mover.MultiPartDownloadInit(srcLoca)
		return mover, err
	case flowtype.STOR_TYPE_HW_OBS, flowtype.STOR_TYPE_HW_FUSIONSTORAGE, flowtype.STOR_TYPE_HW_FUSIONCLOUD:
		mover := &obsmover.ObsMover{}
		err := mover.MultiPartDownloadInit(srcLoca)
		return mover, err
	case flowtype.STOR_TYPE_AZURE_BLOB:
		mover := &blobmover.BlobMover{}
		err := mover.MultiPartDownloadInit(srcLoca)
		return mover, err
	default:
		logger.Printf("Unsupport storType[%s] to init multipart download.\n", srcLoca.StorType)
	}

	return nil,errors.New("Unsupport storage type.")
}

func multiPartUploadInit(objKey string, destLoca *LocationInfo) (mover MoveWorker, err error) {
	switch destLoca.StorType {
	case flowtype.STOR_TYPE_AWS_S3:
		mover := &s3mover.S3Mover{}
		err := mover.MultiPartUploadInit(objKey, destLoca)
		return mover, err
	case flowtype.STOR_TYPE_HW_OBS, flowtype.STOR_TYPE_HW_FUSIONSTORAGE, flowtype.STOR_TYPE_HW_FUSIONCLOUD:
		mover := &obsmover.ObsMover{}
		err := mover.MultiPartUploadInit(objKey, destLoca)
		return mover, err
	case flowtype.STOR_TYPE_AZURE_BLOB:
		mover := &blobmover.BlobMover{}
		err := mover.MultiPartUploadInit(objKey, destLoca)
		return mover, err
	default:
		logger.Printf("Unsupport storType[%s] to download.\n", destLoca.StorType)
	}

	return nil,errors.New("Unsupport storage type.")
}

func abortMultipartUpload(objKey string, destLoca *LocationInfo, mover MoveWorker) error {
	switch destLoca.StorType {
	case flowtype.STOR_TYPE_AWS_S3, flowtype.STOR_TYPE_HW_OBS, flowtype.STOR_TYPE_HW_FUSIONSTORAGE,
	flowtype.STOR_TYPE_HW_FUSIONCLOUD, flowtype.STOR_TYPE_AZURE_BLOB:
		return mover.AbortMultipartUpload(objKey, destLoca)
	default:
		logger.Printf("Unsupport storType[%s] to download.\n", destLoca.StorType)
	}

	return errors.New("Unsupport storage type.")
}

func completeMultipartUpload(objKey string, destLoca *LocationInfo, mover MoveWorker) error {
	switch destLoca.StorType {
	case flowtype.STOR_TYPE_AWS_S3, flowtype.STOR_TYPE_HW_OBS, flowtype.STOR_TYPE_HW_FUSIONSTORAGE,
	flowtype.STOR_TYPE_HW_FUSIONCLOUD, flowtype.STOR_TYPE_AZURE_BLOB:
		return mover.CompleteMultipartUpload(objKey, destLoca)
	default:
		logger.Printf("Unsupport storType[%s] to download.\n", destLoca.StorType)
	}

	return errors.New("Unsupport storage type.")
}

func multipartMoveObj(obj *SourceOject, srcLoca *LocationInfo, destLoca *LocationInfo) error {
	partCount := int64(obj.Obj.Size / PART_SIZE)
	if obj.Obj.Size%PART_SIZE != 0 {
		partCount++
	}

	logger.Printf("multipartMoveObj:obj.Obj.ObjectKey=%s\n",
		obj.Obj.ObjectKey, srcLoca.VirBucket, destLoca.VirBucket)
	downloadObjKey := obj.Obj.ObjectKey
	if srcLoca.VirBucket != "" {
		downloadObjKey = srcLoca.VirBucket + "/" + downloadObjKey
	}
	uploadObjKey := obj.Obj.ObjectKey
	if destLoca.VirBucket != "" {
		uploadObjKey = destLoca.VirBucket + "/" + uploadObjKey
	}

	buf := make([]byte, PART_SIZE)
	var i int64
	var err error
	var uploadMover, downloadMover MoveWorker
	currPartSize := PART_SIZE
	for i = 0; i < partCount; i++ {
		partNumber := i + 1
		offset := int64(i) * PART_SIZE
		if i+1 == partCount {
			currPartSize = obj.Obj.Size - offset
			buf = nil
			buf = make([]byte, currPartSize)
		}

		//download
		start := offset
		end := offset + currPartSize - 1
		if partNumber == 1 {
			downloadMover, err = multiPartDownloadInit(srcLoca)
			if err != nil {
				return err
			}
		}
		readSize, err := downloadMover.DownloadRange(downloadObjKey, srcLoca, buf, start, end)
		if err != nil {
			return errors.New("Download failed.")
		}
		//fmt.Printf("Download part %d range[%d:%d] successfully.\n", partNumber, offset, end)
		if int64(readSize) != currPartSize {
			logger.Printf("Internal error, currPartSize=%d, readSize=%d\n", currPartSize, readSize)
			return errors.New("Internal error")
		}

		//upload
		if partNumber == 1 {
			//init multipart upload
			uploadMover, err = multiPartUploadInit(uploadObjKey, destLoca)
			if err != nil {
				return err
			}
		}
		err1 := uploadMover.UploadPart(uploadObjKey, destLoca, currPartSize, buf, partNumber, offset)
		if err1 != nil {
			err := abortMultipartUpload(obj.Obj.ObjectKey, destLoca, uploadMover)
			if err != nil {
				logger.Printf("Abort s3 multipart upload failed, err:%v\n", err)
			}
			return errors.New("S3 multipart upload failed.")
		}
		//completeParts = append(completeParts, completePart)
	}

	err = completeMultipartUpload(uploadObjKey, destLoca, uploadMover)
	if err != nil {
		logger.Println(err.Error())
	}else {
		logger.Println("Move successfully.")
	}

	return err
}

func deleteObj(ctx context.Context, obj *SourceOject, loca *LocationInfo) error {
	objKey := obj.Obj.ObjectKey
	if loca.VirBucket != "" {
		objKey = loca.VirBucket + "/" + objKey
	}
	var err error = nil
	switch loca.StorType {
	case flowtype.STOR_TYPE_AWS_S3:
		mover := s3mover.S3Mover{}
		err = mover.DeleteObj(objKey, loca)
	case flowtype.STOR_TYPE_HW_OBS, flowtype.STOR_TYPE_HW_FUSIONSTORAGE, flowtype.STOR_TYPE_HW_FUSIONCLOUD:
		mover := obsmover.ObsMover{}
		err = mover.DeleteObj(objKey, loca)
	case flowtype.STOR_TYPE_AZURE_BLOB:
		mover := blobmover.BlobMover{}
		err = mover.DeleteObj(objKey, loca)
	default:
		logger.Printf("Delete object[objkey:%s] from backend storage failed.\n", obj.Obj.ObjectKey)
		err = errors.New("Unspport storage type.")
	}

	if err != nil {
		return err
	}

	//delete metadata
	if loca.VirBucket != "" {
		delMetaReq := osdss3.DeleteObjectInput{Bucket:loca.VirBucket, Key:obj.Obj.ObjectKey}
		_, err = s3client.DeleteObject(ctx, &delMetaReq)
		if err != nil {
			logger.Printf("Delete object metadata of obj[bucket:%s,objKey:%s] failed.\n", loca.VirBucket,
				obj.Obj.ObjectKey)
		}
	}

	return err
}

func move(ctx context.Context, obj *SourceOject, capa chan int64, th chan int,
	srcLoca *LocationInfo, destLoca *LocationInfo, remainSource bool) {
	succeed := true
	if obj.Obj.Backend != srcLoca.BakendId && obj.Obj.Backend != "" {
		//for selfdefined connector, obj.backend and srcLoca.bakendId would be ""
		//TODO: use read/wirte lock
		logger.Printf("Related backend of object is not default.")
		srcLoca = locMap[obj.Obj.Backend]
	}

	//move object
	var err error
	if obj.Obj.Size < ObjSizeLimit {
		err = moveObj(obj, srcLoca, destLoca)
	}else {
		err = multipartMoveObj(obj, srcLoca, destLoca)
	}

	if err != nil {
		succeed = false
	}

	//TODO: what if update meatadata failed
	//add object metadata to the destination bucket if destination is not self-defined
	if succeed && destLoca.VirBucket != "" {
		obj.Obj.BucketName = destLoca.VirBucket
		_,err := s3client.CreateObject(ctx, obj.Obj)
		if err != nil {
			logger.Printf("Add object metadata of obj [objKey:%s] to bucket[name:%s] failed,err:%v.\n", obj.Obj.ObjectKey,
				obj.Obj.BucketName,err)
		}else {
			logger.Printf("Add object metadata of obj [objKey:%s] to bucket[name:%s] succeed.\n", obj.Obj.ObjectKey,
				obj.Obj.BucketName)
		}
	}

	//Delete source data if needed
	if succeed && !remainSource {
		deleteObj(ctx, obj, srcLoca)
		//TODO: what if delete failed
	}

	if succeed {
		//If migrate success, update capacity
		capa <- obj.Obj.Size
	}else {
		capa <- 0
	}
	t := <-th
	logger.Printf("  migrate: consume %d routine.", t)
}

func getOsdsS3Objs(ctx context.Context, conn *pb.Connector, filt *pb.Filter,
	defaultSrcLoca *LocationInfo) ([]*SourceOject, error){
	//TODO:need to support filter
	req := osdss3.ListObjectsRequest{Bucket:conn.BucketName}
	objs,err := s3client.ListObjects(ctx, &req)
	totalObjs := len(objs.ListObjects)
	if err != nil || totalObjs == 0{
		logger.Printf("List objects failed, err:%v\n", err)
		return nil, err
	}
	srcObjs := []*SourceOject{}
	storType := ""
	for i := 0; i < totalObjs; i++ {
		//refresh source location if needed
		if objs.ListObjects[i].Backend != defaultSrcLoca.BakendId && objs.ListObjects[i].Backend != ""{
			//User defined specific backend, which is different from the default backend
			loca,err := refreshBackendLocation(ctx, conn.BucketName, objs.ListObjects[i].Backend)
			if err != nil {
				return nil,err
			}
			storType = loca.StorType
		}else {
			storType = defaultSrcLoca.StorType
		}
		srcObj := SourceOject{}
		srcObj.StorType = storType
		srcObj.Obj = objs.ListObjects[i]
		srcObjs = append(srcObjs, &srcObj)
	}
	return srcObjs,nil
}

func getAwsS3Objs(ctx context.Context, conn *pb.Connector, filt *pb.Filter,
	defaultSrcLoca *LocationInfo) ([]*SourceOject, error) {
	//TODO:need to support filter
	srcObjs := []*SourceOject{}
	objs, err := s3mover.ListObjs(defaultSrcLoca)
	if err != nil {
		return nil, err
	}
	for i := 0; i <len(objs); i++ {
		obj := osdss3.Object{Size:*objs[i].Size, ObjectKey:*objs[i].Key, Backend:""}
		srcObjs = append(srcObjs, &SourceOject{StorType:defaultSrcLoca.StorType, Obj:&obj})
	}
	return srcObjs,nil
}

func getHwObjs(ctx context.Context, conn *pb.Connector, filt *pb.Filter,
	defaultSrcLoca *LocationInfo) ([]*SourceOject, error) {
	//TODO:need to support filter
	srcObjs := []*SourceOject{}
	objs, err := obsmover.ListObjs(defaultSrcLoca)
	if err != nil {
		return nil, err
	}
	for i := 0; i <len(objs); i++ {
		obj := osdss3.Object{Size:objs[i].Size, ObjectKey:objs[i].Key, Backend:""}
		srcObjs = append(srcObjs, &SourceOject{StorType:defaultSrcLoca.StorType, Obj:&obj})
	}
	return srcObjs,nil
}

func getAzureBlobs(ctx context.Context, conn *pb.Connector, filt *pb.Filter,
	defaultSrcLoca *LocationInfo) ([]*SourceOject, error) {
	srcObjs := []*SourceOject{}
	objs, err := blobmover.ListObjs(defaultSrcLoca)
	if err != nil {
		return nil, err
	}
	for i := 0; i <len(objs); i++ {
		obj := osdss3.Object{Size:*objs[i].Properties.ContentLength, ObjectKey:objs[i].Name, Backend:""}
		srcObjs = append(srcObjs, &SourceOject{StorType:defaultSrcLoca.StorType, Obj:&obj})
	}
	return srcObjs,nil
}

func getSourceObjs(ctx context.Context, conn *pb.Connector, filt *pb.Filter,
	defaultSrcLoca *LocationInfo) ([]*SourceOject, error){
	switch conn.Type {
	case flowtype.STOR_TYPE_OPENSDS:
		return getOsdsS3Objs(ctx, conn, filt, defaultSrcLoca)
	case flowtype.STOR_TYPE_AWS_S3:
		return getAwsS3Objs(ctx, conn, filt, defaultSrcLoca)
	case flowtype.STOR_TYPE_HW_OBS, flowtype.STOR_TYPE_HW_FUSIONSTORAGE, flowtype.STOR_TYPE_HW_FUSIONCLOUD:
		return getHwObjs(ctx, conn, filt, defaultSrcLoca)
	case flowtype.STOR_TYPE_AZURE_BLOB:
		return getAzureBlobs(ctx, conn, filt, defaultSrcLoca)
	default:{
		logger.Printf("Unsupport storage type:%v\n", conn.Type)
		return nil, errors.New("unsupport storage type")
	}
	}
	return nil,errors.New("Get source objects failed")
}

func runjob(in *pb.RunJobRequest) error {
	logger.Println("Runjob is called in datamover service.")
	logger.Printf("Request: %+v\n", in)

	j := flowtype.Job{Id:bson.ObjectIdHex(in.Id)}
	j.StartTime = time.Now()

	//TODO:Check if source and destination connectors can access.
	ctx := context.Background()
	_, ok := ctx.Deadline()
	if !ok {
		ctx, _ = context.WithTimeout(ctx, 7200*time.Second)
	}
	//Get source and destination location information
	srcLoca, err := getConnLocation(ctx, in.SourceConn)
	if err != nil {
		j.Status = flowtype.JOB_STATUS_FAILED
		j.EndTime = time.Now()
		db.DbAdapter.UpdateJob(&j)
		logger.Printf("err:%v\n", err)
		return err
	}
	logger.Printf("srcLoca:StorType=%s,VirBucket=%s,BucketName=%s,Region=%s\n",
		srcLoca.StorType, srcLoca.VirBucket, srcLoca.BucketName, srcLoca.Region)
	destLoca, erro := getConnLocation(ctx, in.DestConn)
	if erro != nil {
		j.Status = flowtype.JOB_STATUS_FAILED
		j.EndTime = time.Now()
		db.DbAdapter.UpdateJob(&j)
		return erro
	}
	logger.Printf("destLoca:srcLoca:StorType=%s,VirBucket=%s,BucketName=%s,Region=%s\n",
		destLoca.StorType, destLoca.VirBucket, destLoca.BucketName, destLoca.Region)
	logger.Println("Get connector information succeed.")

	//Get Objects which need to be migrated. Calculate the total number and capacity of objects
	objs,err := getSourceObjs(ctx, in.SourceConn, in.Filt, srcLoca)
	totalObjs := len(objs)
	if err != nil || totalObjs == 0{
		logger.Printf("List objects failed, err:%v\n", err)
		//update database
		j.Status = flowtype.JOB_STATUS_FAILED
		j.EndTime = time.Now()
		db.DbAdapter.UpdateJob(&j)
		return err
	}
	for i := 0; i < totalObjs; i++ {
		j.TotalCount++
		j.TotalCapacity += objs[i].Obj.Size
	}
	logger.Printf("List objects succeed, total count:%d, total capacity:%d\n", j.TotalCount, j.TotalCapacity)
	j.Status = flowtype.JOB_STATUS_RUNNING
	db.DbAdapter.UpdateJob(&j)

	//Make channel
	capa := make(chan int64) //used to transfer capacity of objects
	th := make(chan int, simuRoutines) //concurrent go routines is limited to be  simuRoutines

	//Do migration for each object.
	go doMove(ctx, objs, capa, th, srcLoca, destLoca, in.RemainSource)

	var capacity int64 = 0
	//TODO: What if a part of objects succeed, but the others failed.
	count := 0
	passedCount := 0
	tmout := false
	for ; ;  {
		select {
		case c := <-capa: {//if c equals 0, that means the object is migrated failed.
			capacity += c
			count++

			//TODO: shouled be capacity != 0, capacity >= 0 only for test
			if capacity >= 0 {
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
			logger.Println("Timout.")
		}
		}
		if count >= totalObjs || tmout {
			logger.Printf("break, capacity=%d, timout=%v, count=%d, passed count=%d\n", capacity, tmout, count, passedCount)
			close(capa)
			close(th)
			break
		}
	}

	var ret error = nil
	j.PassedCount = passedCount
	if passedCount < totalObjs {
		errmsg := strconv.Itoa(totalObjs) + " objects, passed " + strconv.Itoa(j.PassedCount)
		logger.Printf("Run job failed: %s\n", errmsg)
		ret = errors.New("failed")
		j.Status = flowtype.JOB_STATUS_FAILED
	}else {
		j.Status = flowtype.JOB_STATUS_SUCCEED
	}

	j.EndTime = time.Now()
	for i := 1; i <= 3; i++ {
		err := db.DbAdapter.UpdateJob(&j)
		if err == nil {
			break
		}
		if i == 3 {
			logger.Printf("Update the finish status of job in database failed three times, no need to try more.")
		}
	}

	return ret
}
