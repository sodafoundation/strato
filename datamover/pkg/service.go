// Copyright (c) 2018 Huawei Technologies Co., Ltd. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pkg

import (
	"context"

	"github.com/micro/go-log"
	pb "github.com/opensds/multi-cloud/datamover/proto"
	osdss3 "github.com/opensds/multi-cloud/s3/proto"
	"github.com/micro/go-micro/client"
	"os"
	"github.com/opensds/multi-cloud/datamover/pkg/db"
	flowtype "github.com/opensds/multi-cloud/dataflow/pkg/type"
	 "github.com/opensds/multi-cloud/dataflow/pkg/utils"
	"errors"
	"github.com/opensds/multi-cloud/backend/proto"
	"time"
	"github.com/globalsign/mgo/bson"
	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
	"github.com/opensds/multi-cloud/datamover/pkg/hw/obs"
	"github.com/opensds/multi-cloud/datamover/pkg/amazon/s3"
	"fmt"
	"strconv"
)

var simuRoutines = 10
var ObjSizeLimit int64 = 50 * 1024 * 1024 //The max object size that can be moved directly
var PART_SIZE int64 = 5 * 1024 * 1024
//format of key:
var locMap map[string]*LocationInfo

type DatamoverService struct{
	s3client osdss3.S3Service
	bkendclient backend.BackendService
}

func NewDatamoverService() pb.DatamoverHandler {
	host := os.Getenv("DB_HOST")
	dbstor := utils.Database{Credential:"unkonwn", Driver:"mongodb", Endpoint:host}
	db.Init(&dbstor)
	locMap = make(map[string]*LocationInfo)

	return &DatamoverService{
		s3client:osdss3.NewS3Service("s3", client.DefaultClient),
		bkendclient:backend.NewBackendService("backend", client.DefaultClient),
	}
}

func (b *DatamoverService) doMove (ctx context.Context, objs []*SourceOject, capa chan int64, th chan int, srcLoca *LocationInfo,
	destLoca *LocationInfo, remainSource bool) {
	//Only three routines allowed to be running at the same time
	//th := make(chan int, simuRoutines)
	for i := 0; i < len(objs); i++ {
		log.Logf("Begin to move obj(key:%s)\n", objs[i].Obj.ObjectKey)
		go b.move(ctx, objs[i], capa, th, srcLoca, destLoca, remainSource)
		//Create one routine
		th <- 1
		log.Log("  doMigrate: produce 1 routine.")
	}
}

func (b *DatamoverService) refreshBackendLocation(ctx context.Context, virtBkname string, bkId string) (*LocationInfo,error) {
	//TODO: use read/write lock to synchronize among routines
	if bkId == "" {
		log.Log("Get backend location failed, because backend id is null.")
		return nil,errors.New("failed")
	}
	loca,exists := locMap[bkId]
	if !exists {
		log.Logf("Backend(id:%s) is not in the map, need to build it.\n", bkId)
		req := backend.GetBackendRequest{Id:bkId}
		bk,err := b.bkendclient.GetBackend(ctx, &req)
		if err != nil {
			log.Logf("Get backend information failed, err:%v\n", err)
			return nil,errors.New("failed")
		} else {
			//TODO:use read/write lock to synchronize among routines
			//TODO: set region to lcation
			loca = &LocationInfo{bk.Backend.Type, bk.Backend.Endpoint, bk.Backend.Endpoint,
			bk.Backend.BucketName, virtBkname,bk.Backend.Access, bk.Backend.Security, bkId}
			//locMap[bkId].region = ""
			//loca = locMap[bkId]
			locMap[bkId] = loca
			log.Logf("Refresh backend[id:%s,name:%s] successfully.\n", bkId, bk.Backend.BucketName)
			return loca,nil
		}
	}else {
		log.Logf("Backend(id:%s) is in the map:%+v\n", loca)
		return loca,nil
	}
}

func (b *DatamoverService) getConnLocation(ctx context.Context, conn *pb.Connector) (*LocationInfo,error) {
	switch conn.Type {
	case flowtype.STOR_TYPE_OPENSDS:{
		virtBkname := conn.GetBucketName()
		reqbk := osdss3.Bucket{Name:virtBkname}
		rspbk,err := b.s3client.GetBucket(ctx, &reqbk)
		if err != nil {
			log.Logf("Get bucket[%s] information failed when refresh connector location.\n", virtBkname)
			return nil,errors.New("get bucket information failed")
		}

		return b.refreshBackendLocation(ctx, virtBkname, rspbk.Backend)
	}
	default:{
		log.Logf("Unsupport type:%s.\n", conn.Type)
		return nil,errors.New("unsupport type")
	}
	}
}

func moveObj(obj *SourceOject, srcLoca *LocationInfo, destLoca *LocationInfo) error {
	//TODO: buf lenght should be the same as obj.size, use 10240 for test only
	buf := make([]byte, 2579)
	//buf := make([]byte, obj.Obj.Size)

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
	default:{
		log.Logf("Not support source backend type:%v\n", srcLoca.StorType)
		err = errors.New("Not support source backend type.")
	}
	}

	if err != nil {
		log.Logf("Download object failed.")
		return err
	}
	log.Logf("Download object succeed, size=%d\n", size)
	log.Logf("buf.len:%d\n", len(buf))

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
	default:
		log.Logf("Not support destination backend type:%v\n", destLoca.StorType)
		return errors.New("Not support destination backend type.")
	}
	if err != nil {
		log.Logf("Upload object[bucket:%s,key:%s] failed, err:%v.\n", destLoca.BucketName, uploadObjKey, err)
	}else {
		log.Logf("Upload object[bucket:%s,key:%s] succeed.\n", destLoca.BucketName, uploadObjKey)
	}


	return nil
}

func downloadRange(objKey string, srcLoca *LocationInfo, buf []byte, start int64, end int64) (size int64, err error) {
	switch srcLoca.StorType {
	case flowtype.STOR_TYPE_AWS_S3:
		mover := &s3mover.S3Mover{}
		return mover.DownloadRange(objKey, srcLoca, buf, start, end)
	case flowtype.STOR_TYPE_HW_OBS, flowtype.STOR_TYPE_HW_FUSIONSTORAGE, flowtype.STOR_TYPE_HW_FUSIONCLOUD:
		mover := &obsmover.ObsMover{}
		return mover.DownloadRange(objKey, srcLoca, buf, start, end)
	default:
		log.Logf("Unsupport storType[%d] to download.\n", srcLoca.StorType)
	}

	return 0,errors.New("Unsupport storage type.")
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
	default:
		log.Logf("Unsupport storType[%d] to download.\n", destLoca.StorType)
	}

	return nil,errors.New("Unsupport storage type.")
}

func uploadPart(objKey string, destLoca *LocationInfo, mover MoveWorker, upBytes int64, buf []byte, partNumber int64, offset int64) error {
	switch destLoca.StorType {
	case flowtype.STOR_TYPE_AWS_S3, flowtype.STOR_TYPE_HW_OBS, flowtype.STOR_TYPE_HW_FUSIONSTORAGE, flowtype.STOR_TYPE_HW_FUSIONCLOUD:
		return mover.UploadPart(objKey, destLoca, upBytes, buf, partNumber, offset)
	default:
		log.Logf("Unsupport storType[%d] to download.\n", destLoca.StorType)
	}

	return errors.New("Unsupport storage type.")
}

func abortMultipartUpload(objKey string, destLoca *LocationInfo, mover MoveWorker) error {
	switch destLoca.StorType {
	case flowtype.STOR_TYPE_AWS_S3, flowtype.STOR_TYPE_HW_OBS, flowtype.STOR_TYPE_HW_FUSIONSTORAGE, flowtype.STOR_TYPE_HW_FUSIONCLOUD:
		return mover.AbortMultipartUpload(objKey, destLoca)
	default:
		log.Logf("Unsupport storType[%d] to download.\n", destLoca.StorType)
	}

	return errors.New("Unsupport storage type.")
}

func completeMultipartUpload(objKey string, destLoca *LocationInfo, mover MoveWorker) error {
	switch destLoca.StorType {
	case flowtype.STOR_TYPE_AWS_S3, flowtype.STOR_TYPE_HW_OBS, flowtype.STOR_TYPE_HW_FUSIONSTORAGE, flowtype.STOR_TYPE_HW_FUSIONCLOUD:
		return mover.CompleteMultipartUpload(objKey, destLoca)
	default:
		log.Logf("Unsupport storType[%d] to download.\n", destLoca.StorType)
	}

	return errors.New("Unsupport storage type.")
}

func multipartMoveObj(obj *SourceOject, srcLoca *LocationInfo, destLoca *LocationInfo) error {
	partCount := int64(obj.Obj.Size / PART_SIZE)
	if obj.Obj.Size%PART_SIZE != 0 {
		partCount++
	}

	downloadObjKey := obj.Obj.ObjectKey
	if srcLoca.VirBucket != "" {
		downloadObjKey = srcLoca.VirBucket + "/" + downloadObjKey
	}
	uploadObjKey := obj.Obj.ObjectKey
	if srcLoca.VirBucket != "" {
		uploadObjKey = srcLoca.VirBucket + "/" + uploadObjKey
	}

	buf := make([]byte, PART_SIZE)
	var i int64
	var upMover MoveWorker
	for i = 0; i < partCount; i++ {
		partNumber := i + 1
		offset := int64(i) * PART_SIZE
		currPartSize := PART_SIZE
		if i+1 == partCount {
			currPartSize = obj.Obj.Size - offset
			buf = nil
			buf = make([]byte, currPartSize)
		}

		start := offset
		end := offset + currPartSize - 1
		readSize, err := downloadRange(downloadObjKey, srcLoca, buf, start, end)
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
			upMover, err = multiPartUploadInit(uploadObjKey, destLoca)
			if err != nil {
				return err
			}
		}

		err1 := uploadPart(uploadObjKey, destLoca, upMover, currPartSize, buf, partNumber, offset)
		if err1 != nil {
			err := abortMultipartUpload(obj.Obj.ObjectKey, destLoca, upMover)
			if err != nil {
				fmt.Printf("Abort s3 multipart upload failed, err:%v\n", err)
			}
			return errors.New("S3 multipart upload failed.")
		}
		//completeParts = append(completeParts, completePart)
	}

	err := completeMultipartUpload(uploadObjKey, destLoca, upMover)
	if err != nil {
		fmt.Println(err.Error())
	}else {
		fmt.Println("Move successfully.")
	}

	return err
}

func (b *DatamoverService) deleteObj(ctx context.Context, obj *SourceOject, loca *LocationInfo) error {
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
	default:
		log.Logf("Delete object[objkey:%s] from backend storage failed.\n", obj.Obj.ObjectKey)
		err = errors.New("Unspport storage type.")
	}

	if err != nil {
		return err
	}

	//delete metadata
	delMetaReq := osdss3.DeleteObjectInput{Bucket:loca.BucketName, Key:obj.Obj.ObjectKey}
	_, err = b.s3client.DeleteObject(ctx, &delMetaReq)
	if err != nil {
		log.Logf("Delete object metadata of obj [objKey:%s] failed.\n", obj.Obj.ObjectKey)
	}

	return err
}

func (b *DatamoverService) move(ctx context.Context, obj *SourceOject, capa chan int64, th chan int,
	srcLoca *LocationInfo, destLoca *LocationInfo, remainSource bool) {
	succeed := true
	if obj.Obj.Backend != srcLoca.BakendId && obj.Obj.Backend != "" {
		//for selfdefined connector, obj.backend and srcLoca.bakendId would be ""
		//TODO: use read/wirte lock
		log.Logf("Related backend of object is not default.")
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
		_,err := b.s3client.CreateObject(ctx, obj.Obj)
		if err != nil {
			log.Logf("Add object metadata of obj [objKey:%s] to bucket[name:%s] failed,err:%v.\n", obj.Obj.ObjectKey,
				obj.Obj.BucketName,err)
		}
	}

	//Delete source data if needed
	if succeed && !remainSource {
		b.deleteObj(ctx, obj, srcLoca)
		//TODO: what if delete failed
	}

	if succeed {
		//If migrate success, update capacity
		capa <- obj.Obj.Size
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
		//obj := osdss3.Object{ObjectKey:filt.Prefix, BucketName:conn.BucketName}
		//TODO:need to support filter
		req := osdss3.ListObjectsRequest{Bucket:conn.BucketName}
		objs,err := b.s3client.ListObjects(ctx, &req)
		totalObjs := len(objs.ListObjects)
		if err != nil || totalObjs == 0{
			log.Logf("List objects failed, err:%v\n", err)
			return nil, err
		}
		srcObjs := []*SourceOject{}
		storType := ""
		for i := 0; i < totalObjs; i++ {
			//refresh source location if needed
			if objs.ListObjects[i].Backend != defaultSrcLoca.BakendId && objs.ListObjects[i].Backend != ""{
				//User defined specific backend, which is different from the default backend
				loca,err := b.refreshBackendLocation(ctx, conn.BucketName, objs.ListObjects[i].Backend)
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
	default:{
		log.Logf("Unsupport storage type:%v\n", conn.Type)
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
	srcLoca, err := b.getConnLocation(ctx, in.SourceConn)
	if err != nil {
		j.Status = flowtype.JOB_STATUS_FAILED
		j.EndTime = time.Now()
		db.DbAdapter.UpdateJob(&j)
		log.Logf("err:%v\n", err)
		return err
	}
	destLoca, erro := b.getConnLocation(ctx, in.DestConn)
	if erro != nil {
		j.Status = flowtype.JOB_STATUS_FAILED
		j.EndTime = time.Now()
		db.DbAdapter.UpdateJob(&j)
		return erro
	}
	log.Log("Get connector information succeed.")

	//Get Objects which need to be migrated. Calculate the total number and capacity of objects
	objs,err := b.getSourceObjs(ctx, in.SourceConn, in.Filt, srcLoca)
	totalObjs := len(objs)
	if err != nil || totalObjs == 0{
		log.Logf("List objects failed, err:%v\n", err)
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
	log.Logf("List objects succeed, total count:%d, total capacity:%d\n", j.TotalCount, j.TotalCapacity)
	j.Status = flowtype.JOB_STATUS_RUNNING
	db.DbAdapter.UpdateJob(&j)

	//Make channel
	capa := make(chan int64) //used to transfer capacity of objects
	th := make(chan int, simuRoutines) //concurrent go routines is limited to be  simuRoutines

	//Do migration for each object.
	go b.doMove(ctx, objs, capa, th, srcLoca, destLoca, in.RemainSource)

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
			log.Log("Timout.")
		}
		}
		if count >= totalObjs || tmout {
			if tmout {
				out.Err = "timeout"
			}
			log.Logf("break, capacity=%d, timout=%v, count=%d, passed count=%d\n", capacity, tmout, count, passedCount)
			close(capa)
			close(th)
			break
		}
	}

	var ret error = nil
	j.PassedCount = passedCount
	if passedCount < totalObjs {
		errmsg := strconv.Itoa(totalObjs) + " objects, passed " + strconv.Itoa(j.PassedCount)
		log.Logf("Run job failed: %s\n", errmsg)
		out.Err = errmsg
		ret = errors.New("failed")
		j.Status = flowtype.JOB_STATUS_FAILED
	}else {
		out.Err = "success"
		j.Status = flowtype.JOB_STATUS_SUCCEED
	}

	j.EndTime = time.Now()
	for i := 1; i <= 3; i++ {
		err := db.DbAdapter.UpdateJob(&j)
		if err == nil {
			break
		}
		if i == 3 {
			log.Logf("Update the finish status of job in database failed three times, no need to try more.")
		}
	}

	return ret
}
