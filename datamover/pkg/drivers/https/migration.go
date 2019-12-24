// Copyright 2019 The OpenSDS Authors.
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

package migration

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"time"

	"github.com/globalsign/mgo/bson"
	"github.com/micro/go-micro/client"
	"github.com/micro/go-micro/metadata"
	"github.com/opensds/multi-cloud/api/pkg/common"
	"github.com/opensds/multi-cloud/backend/proto"
	flowtype "github.com/opensds/multi-cloud/dataflow/pkg/model"
	"github.com/opensds/multi-cloud/datamover/pkg/db"
	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
	pb "github.com/opensds/multi-cloud/datamover/proto"
	s3utils "github.com/opensds/multi-cloud/s3/pkg/utils"
	osdss3 "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

var simuRoutines = 10
var PART_SIZE int64 = 16 * 1024 * 1024 //The max object size that can be moved directly, default is 16M.
var JOB_RUN_TIME_MAX = 86400           //seconds, equals 1 day
var s3client osdss3.S3Service
var bkendclient backend.BackendService
var MiniSpeed int64 = 5 // 5KByte/Sec

const WT_MOVE = 96
const WT_DELETE = 4
const JobType = "migration"

type Migration interface {
	Init()
	HandleMsg(msg string)
}

func Init() {
	log.Infof("Migration init.")
	s3client = osdss3.NewS3Service("s3", client.DefaultClient)
	bkendclient = backend.NewBackendService("backend", client.DefaultClient)
}

func HandleMsg(msgData []byte) error {
	var job pb.RunJobRequest
	err := json.Unmarshal(msgData, &job)
	if err != nil {
		log.Infof("unmarshal failed, err:%v\n", err)
		return err
	}

	//Check the status of job, and run it if needed
	status := db.DbAdapter.GetJobStatus(job.Id)
	if status != flowtype.JOB_STATUS_PENDING {
		log.Infof("job[id#%s] is not in %s status.\n", job.Id, flowtype.JOB_STATUS_PENDING)
		return nil //No need to consume this message again
	}

	log.Infof("HandleMsg:job=%+v\n", job)
	go runjob(&job)
	return nil
}

func doMigrate(ctx context.Context, objs []*osdss3.Object, capa chan int64, th chan int, req *pb.RunJobRequest,
	job *flowtype.Job) {
	for i := 0; i < len(objs); i++ {
		if objs[i].Tier == s3utils.Tier999 {
			// archived object cannot be moved currently
			log.Warnf("Object(key:%s) is archived, cannot be migrated.\n", objs[i].ObjectKey)
			continue
		}
		log.Infof("************Begin to move obj(key:%s)\n", objs[i].ObjectKey)

		//Create one routine
		go migrate(ctx, objs[i], capa, th, req, job)
		th <- 1
		log.Infof("doMigrate: produce 1 routine, len(th):%d.\n", len(th))
	}
}

func CopyObj(ctx context.Context, obj *osdss3.Object, destLoca *LocationInfo, job *flowtype.Job) error {
	log.Infof("*****Move object, size is %d.\n", obj.Size)
	if obj.Size <= 0 {
		return nil
	}

	req := &osdss3.CopyObjectRequest{
		SrcObjectName:    obj.ObjectKey,
		SrcBucketName:    obj.BucketName,
		TargetBucketName: destLoca.BucketName,
		TargetObjectName: obj.ObjectKey,
	}
	tmoutSec := obj.Size / MiniSpeed
	opt := client.WithRequestTimeout(time.Duration(tmoutSec) * time.Second)
	_, err := s3client.CopyObject(ctx, req, opt)
	if err != nil {
		log.Errorf("copy object[%s] failed, err:%v\n", obj.ObjectKey, err)
	}

	progress(job, obj.Size, WT_MOVE)

	return err
}

func MultipartCopyObj(ctx context.Context, obj *osdss3.Object, destLoca *LocationInfo, job *flowtype.Job) error {
	partCount := int64(obj.Size / PART_SIZE)
	if obj.Size%PART_SIZE != 0 {
		partCount++
	}

	log.Infof("*****Copy object[%s] from #%s# to #%s#, size=%d, partCount=%d.\n", obj.ObjectKey, obj.BucketName,
		destLoca.BucketName, obj.Size, partCount)

	var i int64
	var err error
	var uploadId string
	var initSucceed bool = false
	var completeParts []*osdss3.CompletePart
	currPartSize := PART_SIZE
	for i = 0; i < partCount; i++ {
		partNumber := i + 1
		offset := int64(i) * PART_SIZE
		if i+1 == partCount {
			currPartSize = obj.Size - offset
		}

		if partNumber == 1 {
			// init upload
			rsp, err := s3client.InitMultipartUpload(ctx, &osdss3.InitMultiPartRequest{
				BucketName: destLoca.BucketName,
				ObjectKey:  obj.ObjectKey,
				Tier:       destLoca.Tier,
				Location:   destLoca.BakendName,
				Attrs:      obj.CustomAttributes,
				// TODO: add content-type
			})
			if err != nil {
				log.Errorf("init mulipart upload failed:%v\n", err)
				break
			}
			initSucceed = true
			uploadId = rsp.UploadID
			log.Debugln("**** init multipart upload succeed, uploadId=", uploadId)
		}

		// copy part
		copyReq := &osdss3.CopyObjPartRequest{SourceBucket: obj.BucketName, SourceObject: obj.ObjectKey,
			TargetBucket: destLoca.BucketName, TargetObject: obj.ObjectKey, PartID: partNumber,
			UploadID: uploadId, ReadOffset: offset, ReadLength: currPartSize,
		}
		var rsp *osdss3.CopyObjPartResponse
		try := 0
		tmoutSec := currPartSize / MiniSpeed
		opt := client.WithRequestTimeout(time.Duration(tmoutSec) * time.Second)
		for try < 3 { // try 3 times in case network is not stable
			log.Debugf("###copy object part, objkey=%s, uploadid=%s, offset=%d, lenth=%d\n", obj.ObjectKey, uploadId, offset, currPartSize)
			rsp, err = s3client.CopyObjPart(ctx, copyReq, opt)
			if err == nil {
				log.Debugln("copy part succeed")
				break
			} else {
				log.Warnf("copy part failed, err:%v\n", err)
			}
			try++
		}
		if try == 3 {
			log.Errorln("copy part failed too many times")
			break
		}

		log.Debugf("copy part[obj=%s, uploadId=%s, ReadOffset=%d, ReadLength=%d] succeed\n", obj.ObjectKey,
			uploadId, offset, currPartSize)
		completePart := &osdss3.CompletePart{PartNumber: partNumber, ETag: rsp.Etag}
		completeParts = append(completeParts, completePart)

		// update job progress
		if job != nil {
			log.Debugln("update job")
			progress(job, currPartSize, WT_MOVE)
		}
	}

	if err == nil {
		// copy parts succeed, need to complete it
		completeReq := &osdss3.CompleteMultipartRequest{BucketName: destLoca.BucketName, ObjectKey: obj.ObjectKey,
			UploadId: uploadId, CompleteParts: completeParts, SourceVersionID: obj.VersionId}
		if job == nil {
			// this is for lifecycle management
			completeReq.RequestType = s3utils.RequestType_Lifecycle
		}
		_, err = s3client.CompleteMultipartUpload(ctx, completeReq)
		if err != nil {
			log.Errorf("complete multipart copy failed, err:%v\n", err)
		}
	}

	if err != nil {
		if initSucceed == true {
			log.Debugf("abort multipart copy, bucket:%s, object:%s, uploadid:%s\n", destLoca.BucketName, obj.ObjectKey, uploadId)
			_, ierr := s3client.AbortMultipartUpload(ctx, &osdss3.AbortMultipartRequest{BucketName: destLoca.BucketName,
				ObjectKey: obj.ObjectKey, UploadId: uploadId,
			})
			if ierr != nil {
				// it shoud be cleaned by gc in s3 service
				log.Warnf("abort multipart copy failed, err:%v\n", ierr)
			}
		}

		return err
	}

	log.Infof("*****Copy object[%s] from #%s# to #%s# succeed.\n", obj.ObjectKey, obj.Location,
		destLoca.BakendName)
	return nil
}

func deleteObj(ctx context.Context, obj *osdss3.Object) error {
	delMetaReq := osdss3.DeleteObjectInput{Bucket: obj.BucketName, Key: obj.ObjectKey}
	_, err := s3client.DeleteObject(ctx, &delMetaReq)
	if err != nil {
		log.Infof("delete object[bucket:%s,objKey:%s] failed, err:%v\n", obj.BucketName,
			obj.ObjectKey, err)
	} else {
		log.Infof("Delete object[bucket:%s,objKey:%s] successfully.\n", obj.BucketName,
			obj.ObjectKey)
	}

	return err
}

func migrate(ctx context.Context, obj *osdss3.Object, capa chan int64, th chan int, req *pb.RunJobRequest, job *flowtype.Job) {
	log.Infof("Move obj[%s] from bucket[%s] to bucket[%s].\n",
		obj.ObjectKey, job.SourceLocation, job.DestLocation)

	succeed := true

	// copy object
	var err error
	PART_SIZE = GetMultipartSize()
	destLoc := &LocationInfo{BucketName: req.DestConn.BucketName, Tier: obj.Tier}
	if obj.Size <= PART_SIZE {
		err = CopyObj(ctx, obj, destLoc, job)
	} else {
		err = MultipartCopyObj(ctx, obj, destLoc, job)
	}

	if err != nil {
		succeed = false
	}

	if succeed && !req.RemainSource {
		deleteObj(ctx, obj)
		// TODO: Need to clean if delete failed.
	}

	if succeed {
		//If migrate success, update capacity
		log.Infof("  migrate object[key=%s,versionid=%s] succeed.", obj.ObjectKey, obj.VersionId)
		capa <- obj.Size
		if job.Type == "migration" {
			progress(job, obj.Size, WT_DELETE)
		}
	} else {
		log.Infof("  migrate object[key=%s,versionid=%s] failed.", obj.ObjectKey, obj.VersionId)
		capa <- -1
	}

	t := <-th
	log.Infof("  migrate: consume %d routine, len(th)=%d\n", t, len(th))
}

func updateJob(j *flowtype.Job) {
	for i := 1; i <= 3; i++ {
		err := db.DbAdapter.UpdateJob(j)
		if err == nil {
			break
		}
		if i == 3 {
			log.Infof("update the finish status of job in database failed three times, no need to try more.")
		}
	}
}

func initJob(ctx context.Context, in *pb.RunJobRequest, j *flowtype.Job) error {
	j.Status = flowtype.JOB_STATUS_RUNNING
	j.SourceLocation = in.SourceConn.BucketName
	j.DestLocation = in.DestConn.BucketName
	j.Type = JobType

	// get total count and total size of objects need to be migrated
	totalCount, totalSize, err := countObjs(ctx, in)
	if err != nil || totalCount == 0 {
		if err != nil {
			j.Status = flowtype.JOB_STATUS_FAILED
		} else {
			j.Status = flowtype.JOB_STATUS_SUCCEED
		}
		j.EndTime = time.Now()
		updateJob(j)
		log.Infof("err:%v, totalCount=%d\n", err, totalCount)
		return errors.New("no need move")
	}

	j.TotalCount = totalCount
	j.TotalCapacity = totalSize
	updateJob(j)

	return nil
}

func runjob(in *pb.RunJobRequest) error {
	log.Infoln("Runjob is called in datamover service.")
	log.Infof("Request: %+v\n", in)

	// set context tiemout
	ctx := metadata.NewContext(context.Background(), map[string]string{
		common.CTX_KEY_USER_ID:   in.UserId,
		common.CTX_KEY_TENANT_ID: in.TenanId,
	})
	// 60 means 1 minute, 2592000 means 30 days, 86400 means 1 day
	dur := GetCtxTimeout("JOB_MAX_RUN_TIME", 60, 2592000, 86400)
	_, ok := ctx.Deadline()
	if !ok {
		ctx, _ = context.WithTimeout(ctx, dur)
	}

	// init job
	j := flowtype.Job{Id: bson.ObjectIdHex(in.Id)}
	j.StartTime = time.Now()
	j.Status = flowtype.JOB_STATUS_RUNNING
	err := initJob(ctx, in, &j)
	if err != nil {
		return err
	}

	// used to transfer capacity(size) of objects
	capa := make(chan int64)
	// concurrent go routines is limited to be simuRoutines
	th := make(chan int, simuRoutines)
	var limit int32 = 1000
	var marker string
	for {
		objs, err := getObjs(ctx, in, marker, limit)
		if err != nil {
			//update database
			j.Status = flowtype.JOB_STATUS_FAILED
			j.EndTime = time.Now()
			db.DbAdapter.UpdateJob(&j)
			return err
		}

		num := len(objs)
		if num == 0 {
			break
		}

		//Do migration for each object.
		go doMigrate(ctx, objs, capa, th, in, &j)

		if num < int(limit) {
			break
		}
		marker = objs[num-1].ObjectKey
	}

	var capacity, count, passedCount, totalObjs int64 = 0, 0, 0, j.TotalCount
	tmout := false
	for {
		select {
		case c := <-capa:
			{ //if c is less than 0, that means the object is migrated failed.
				count++
				if c >= 0 {
					passedCount++
					capacity += c
				}

				//update database
				j.PassedCount = passedCount
				j.PassedCapacity = capacity
				log.Infof("ObjectMigrated:%d,TotalCapacity:%d Progress:%d\n", j.PassedCount, j.TotalCapacity, j.Progress)
				db.DbAdapter.UpdateJob(&j)
			}
		case <-time.After(dur):
			{
				tmout = true
				log.Warnln("Timout.")
			}
		}
		if count >= totalObjs || tmout {
			log.Infof("break, capacity=%d, timout=%v, count=%d, passed count=%d\n", capacity, tmout, count, passedCount)
			close(capa)
			close(th)
			break
		}
	}

	var ret error = nil
	j.PassedCount = int64(passedCount)
	if passedCount < totalObjs {
		errmsg := strconv.FormatInt(totalObjs, 10) + " objects, passed " + strconv.FormatInt(passedCount, 10)
		log.Infof("run job failed: %s\n", errmsg)
		ret = errors.New("failed")
		j.Status = flowtype.JOB_STATUS_FAILED
	} else {
		j.Status = flowtype.JOB_STATUS_SUCCEED
	}

	j.EndTime = time.Now()
	for i := 1; i <= 3; i++ {
		err := db.DbAdapter.UpdateJob(&j)
		if err == nil {
			break
		}
		if i == 3 {
			log.Infof("update the finish status of job in database failed three times, no need to try more.")
		}
	}

	return ret
}

// To calculate Progress of migration process
func progress(job *flowtype.Job, size int64, wt float64) {
	// Migrated Capacity = Old_migrated capacity + WT(Process)*Size of Object/100
	MigratedCapacity := job.MigratedCapacity + float64(size)*(wt/100)
	job.MigratedCapacity = math.Round(MigratedCapacity*100) / 100
	// Progress = Migrated Capacity*100/ Total Capacity
	job.Progress = int64(job.MigratedCapacity * 100 / float64(job.TotalCapacity))
	log.Debugf("Progress %d, MigratedCapacity %d, TotalCapacity %d\n", job.Progress, job.MigratedCapacity, job.TotalCapacity)
	db.DbAdapter.UpdateJob(job)
}
