// Copyright 2019 The soda Authors.
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

package plan

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"time"

	"github.com/soda/multi-cloud/api/pkg/common"
	"github.com/soda/multi-cloud/dataflow/pkg/db"
	"github.com/soda/multi-cloud/dataflow/pkg/kafka"
	. "github.com/soda/multi-cloud/dataflow/pkg/model"
	"github.com/soda/multi-cloud/dataflow/pkg/scheduler/trigger"
	datamover "github.com/soda/multi-cloud/datamover/proto"

	"github.com/globalsign/mgo/bson"
	"github.com/micro/go-micro/v2/metadata"
	log "github.com/sirupsen/logrus"
)

var tblConnector = "connector"
var tblPolicy = "policy"
var topicMigration = "migration"

const (
	BIT_REGION     = 1  //00001
	BIT_ENDPOINT   = 2  //00010
	BIT_BUCKETNAME = 4  //00100
	BIT_ACCESS     = 8  //01000
	BIT_SECURITY   = 16 //10000
	BIT_FULL       = 31 //11111
)

func isEqual(src *Connector, dest *Connector) bool {
	switch src.StorType {
	case STOR_TYPE_soda:
		if dest.StorType == STOR_TYPE_soda && src.BucketName == dest.BucketName {
			return true
		} else {
			return false
		}
	default: //TODO: check according to StorType later.
		return false
	}
}

func checkConnValidation(conn *Connector) error {
	var flag int
	if conn.StorType == STOR_TYPE_soda {
		//If StorType is soda-obj, no connector needed.
		return nil
	}
	if conn.StorType == STOR_TYPE_CEPH_S3 {
		flag = 1 //region is not a required param for ceph
	} else {
		flag = 0
	}

	cfg := conn.ConnConfig
	for i := 0; i < len(cfg); i++ {
		switch cfg[i].Key {
		case "region":
			flag = flag | BIT_REGION
		case "endpoint":
			flag = flag | BIT_ENDPOINT
		case "bucketname":
			flag = flag | BIT_BUCKETNAME
		case "access":
			flag = flag | BIT_ACCESS
		case "security":
			flag = flag | BIT_SECURITY
		default:
			log.Infof("Uknow key[%s] for connector.\n", cfg[i].Key)
		}
	}
	if flag != BIT_FULL {
		log.Errorf("invalid connector, flag=%b\n", flag)
		return errors.New("invalid connector")
	}

	return nil
}

func Create(ctx context.Context, plan *Plan) (*Plan, error) {
	//Check parameter validity
	m, err := regexp.MatchString("[[:alnum:]-_.]+", plan.Name)
	if !m {
		log.Errorf("Invalid plan name[%s], err:%v", plan.Name, err)
		return nil, ERR_INVALID_PLAN_NAME
	}

	plan.Id = ""
	plan.LastSchedTime = 0 //set to be 0 as default

	if isEqual(&plan.SourceConn, &plan.DestConn) {
		log.Info("source connector is the same as destination connector.")
		return nil, ERR_DEST_SRC_CONN_EQUAL
	}
	if checkConnValidation(&plan.SourceConn) != nil {
		log.Errorf("Source connector is invalid, type=%s\n", plan.SourceConn.StorType)
		return nil, ERR_SRC_CONN_NOT_EXIST
	}
	if checkConnValidation(&plan.DestConn) != nil {
		log.Errorf("Target connector is invalid, type=%s\n", plan.DestConn.StorType)
		return nil, ERR_DEST_CONN_NOT_EXIST
	}

	//Add to database
	plan, err = db.DbAdapter.CreatePlan(ctx, plan)
	if err != nil {
		log.Errorf("create plan in db failed,%v", err)
		return nil, err
	}

	if plan.PolicyId != "" && plan.PolicyEnabled {
		if err := trigger.GetTriggerMgr().Add(ctx, plan, NewPlanExecutor(plan)); err != nil {
			log.Errorf("Add plan(%s) to trigger failed, %v", plan.Id.Hex(), err)
			return nil, err
		}
	}

	return plan, nil
}

func Delete(ctx context.Context, id string) error {

	plan, err := db.DbAdapter.GetPlan(ctx, id)
	if err == ERR_PLAN_NOT_EXIST {
		log.Errorf("specified plan(%s) is not exist, ignore it ", id)
		return nil
	}

	if err != nil {
		log.Errorf("Delete plan failed, %v", err)
		return err
	}

	if plan.PolicyId != "" {
		err = trigger.GetTriggerMgr().Remove(ctx, plan)
		if err != nil && err != ERR_PLAN_NOT_EXIST {
			log.Errorf("Remove plan from triggers failed, %v", err)
			return err
		}
	}

	return db.DbAdapter.DeletePlan(ctx, id)
}

//1. cannot update type
func Update(ctx context.Context, planId string, updateMap map[string]interface{}) (*Plan, error) {

	curPlan, err := db.DbAdapter.GetPlan(ctx, planId)
	if err != nil {
		log.Errorf("Update plan failed, err: can not get the plan(%v).\n", err.Error())
		return nil, err
	}

	if v, ok := updateMap["name"]; ok {
		name := v.(string)
		m, err := regexp.MatchString("[[:alnum:]-_.]+", name)
		if !m {
			log.Errorf("Invalid plan name[%s],err:", name, err) //cannot use all as name
			return nil, ERR_INVALID_PLAN_NAME
		}
		curPlan.Name = name
	}

	if v, ok := updateMap["sourceConn"]; ok {
		b, _ := json.Marshal(v)
		curPlan.SourceConn = Connector{}
		json.Unmarshal(b, &curPlan.SourceConn)
	}

	if v, ok := updateMap["destConn"]; ok {
		b, _ := json.Marshal(v)
		curPlan.DestConn = Connector{}
		json.Unmarshal(b, &curPlan.DestConn)
	}

	if v, ok := updateMap["filter"]; ok {
		b, _ := json.Marshal(v)
		curPlan.Filter = Filter{Prefix: "/"}
		json.Unmarshal(b, &curPlan.Filter)
	}

	if isEqual(&curPlan.SourceConn, &curPlan.DestConn) {
		log.Info("source connector is the same as destination connector.")
		return nil, ERR_DEST_SRC_CONN_EQUAL
	}

	var needUpdateTrigger = false
	if v, ok := updateMap["policyEnabled"]; ok {
		curPlan.PolicyEnabled = v.(bool)
		needUpdateTrigger = true
	}

	if v, ok := updateMap["policyId"]; ok {
		curPlan.PolicyId = v.(string)
		needUpdateTrigger = true
	}

	if v, ok := updateMap["description"]; ok {
		curPlan.Description = v.(string)
	}

	if v, ok := updateMap["remainSource"]; ok {
		curPlan.RemainSource = v.(bool)
	}

	if needUpdateTrigger {
		trigger.GetTriggerMgr().Remove(ctx, curPlan)
		if curPlan.PolicyId != "" && curPlan.PolicyEnabled {
			if err := trigger.GetTriggerMgr().Add(ctx, curPlan, NewPlanExecutor(curPlan)); err != nil {
				log.Errorf("Add plan(%s) to trigger failed, %v", curPlan.Id.Hex(), err)
				return nil, err
			}
		}
	}

	return db.DbAdapter.UpdatePlan(ctx, curPlan)
}

func Get(ctx context.Context, id string) (*Plan, error) {
	return db.DbAdapter.GetPlan(ctx, id)
}

func List(ctx context.Context, limit int, offset int, filter interface{}) ([]Plan, error) {
	return db.DbAdapter.ListPlan(ctx, limit, offset, filter)
}

func getLocation(conn *Connector) (string, error) {
	switch conn.StorType {
	case STOR_TYPE_soda:
		return conn.BucketName, nil
	case STOR_TYPE_HW_OBS, STOR_TYPE_AWS_S3, STOR_TYPE_HW_FUSIONSTORAGE, STOR_TYPE_HW_FUSIONCLOUD,
		STOR_TYPE_AZURE_BLOB, STOR_TYPE_CEPH_S3, STOR_TYPE_GCP_S3, STOR_TYPE_IBM_COS, STOR_TYPE_ALIBABA_OSS, STOR_TYPE_SONY_ODA:
		cfg := conn.ConnConfig
		for i := 0; i < len(cfg); i++ {
			if cfg[i].Key == "bucketname" {
				return cfg[i].Value, nil
			}
		}
		return "", errors.New("no bucket provided for self-defined connector.")
	default:
		log.Errorf("unsupport cnnector type:%v, return ERR_INNER_ERR\n", conn.StorType)
		return "", ERR_INNER_ERR
	}
}

func sendJob(req *datamover.RunJobRequest) error {
	data, err := json.Marshal(*req)
	if err != nil {
		log.Errorf("Marshal run job request failed, err:%v\n", data)
		return err
	}

	return kafka.ProduceMsg(topicMigration, data)
}

func buildConn(reqConn *datamover.Connector, conn *Connector) {
	if conn.StorType == STOR_TYPE_soda {
		reqConn.BucketName = conn.BucketName
	} else {
		for i := 0; i < len(conn.ConnConfig); i++ {
			reqConn.ConnConfig = append(reqConn.ConnConfig, &datamover.KV{Key: conn.ConnConfig[i].Key, Value: conn.ConnConfig[i].Value})
		}
	}
}

func Run(planId, tenantId, userId string) (bson.ObjectId, error) {
	ctx := metadata.NewContext(context.Background(), map[string]string{
		common.CTX_KEY_USER_ID:   userId,
		common.CTX_KEY_TENANT_ID: tenantId,
	})
	//Get information from database
	plan, err := db.DbAdapter.GetPlan(ctx, planId)
	if err != nil {
		return "", err
	}

	//scheduling must be mutual excluded among several schedulers
	//Get Lock
	ret := db.DbAdapter.LockSched(tenantId, string(plan.Id.Hex()))
	for i := 0; i < 3; i++ {
		if ret == LockSuccess {
			//Make sure unlock before return
			defer db.DbAdapter.UnlockSched(tenantId, string(plan.Id.Hex()))
			break
		} else if ret == LockBusy {
			return "", ERR_RUN_PLAN_BUSY
		} else {
			//Try to lock again, try three times at most
			ret = db.DbAdapter.LockSched(tenantId, string(plan.Id.Hex()))
		}
	}

	//Get source location by source connector
	srcLocation, err1 := getLocation(&plan.SourceConn)
	if err1 != nil {
		log.Errorf("Run plan failed, invalid source connector, type=%s\n", plan.SourceConn.StorType)
		return "", err1
	}

	//Get destination location by destination connector
	destLocation, err2 := getLocation(&plan.DestConn)
	if err2 != nil {
		log.Errorf("Run plan failed, invalid target connector, type=%s\n", plan.DestConn.StorType)
		return "", err2
	}

	ct := time.Now()
	//Create job
	job := Job{}
	job.Type = plan.Type
	job.PlanId = string(plan.Id.Hex())
	job.PlanName = plan.Name
	job.SourceLocation = srcLocation
	job.DestLocation = destLocation
	job.CreateTime = ct
	job.Status = JOB_STATUS_PENDING
	job.RemainSource = plan.RemainSource
	job.StartTime = time.Time{}
	job.TenantId = tenantId
	job.UserId = userId
	//add job to database
	_, err = db.DbAdapter.CreateJob(ctx, &job)
	if err == nil {
		//TODO: change to send job to datamover by kafka
		//This way send job is the temporary
		filt := datamover.Filter{Prefix: plan.Filter.Prefix}
		req := datamover.RunJobRequest{
			Id: job.Id.Hex(), TenanId: tenantId, UserId: userId,
			RemainSource: plan.RemainSource, Filt: &filt,
		}
		srcConn := datamover.Connector{Type: plan.SourceConn.StorType}
		buildConn(&srcConn, &plan.SourceConn)
		req.SourceConn = &srcConn
		destConn := datamover.Connector{Type: plan.DestConn.StorType}
		buildConn(&destConn, &plan.DestConn)
		req.DestConn = &destConn
		req.RemainSource = job.RemainSource
		go sendJob(&req)
	} else {
		log.Infof("Add job[id=%s,plan=%s,source_location=%s,dest_location=%s] to database failed.\n", string(job.Id.Hex()),
			job.PlanName, job.SourceLocation, job.DestLocation)
	}

	return job.Id, nil
}

type TriggerExecutor struct {
	planId   string
	tenantId string
	userId   string
}

func NewPlanExecutor(plan *Plan) trigger.Executer {
	return &TriggerExecutor{
		planId:   plan.Id.Hex(),
		tenantId: plan.TenantId,
		userId:   plan.UserId,
	}
}

func (p *TriggerExecutor) Run() {
	log.Infof("schedudler run plan (%s) is called in dataflow service.", p.planId)
	jobId, err := Run(p.planId, p.tenantId, p.userId)
	if err != nil {
		log.Errorf("PlanExcutor run plan(%s) error, jobid:%s, error:%v", p.planId, jobId, err)
	}
}
