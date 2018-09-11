package plan

import (
	"regexp"
	"github.com/micro/go-log"
	"github.com/opensds/go-panda/dataflow/pkg/db"
	"github.com/globalsign/mgo"
	. "github.com/opensds/go-panda/dataflow/pkg/type"
	"github.com/globalsign/mgo/bson"
	"github.com/opensds/go-panda/datamover/proto"
	"golang.org/x/net/context"
	"time"
	"reflect"
	"github.com/opensds/go-panda/dataflow/pkg/type"
)

var dataBaseName = "test"
var tblConnector = "connector"
var tblPolicy = "policy"

func Create(plan *Plan) error {
	//Check parameter validity
	m, err := regexp.MatchString("[[:alnum:]-_.]+", plan.Name)
	if !m || plan.Name == "all"{
		log.Logf("Invalid plan name[%s], err:%v", plan.Name, err)
		return ERR_INVALID_PLAN_NAME
	}

	plan.Id = ""
	//plan.IsSched = false //set to be false as default
	//plan.SchedServer = "" // set to be null as default
	plan.LastSchedTime = 0 //set to be 0 as default

	if plan.PolicyId != "" {
		if bson.IsObjectIdHex(plan.PolicyId) {
			plan.PolicyRef = mgo.DBRef{tblPolicy, bson.ObjectIdHex(plan.PolicyId), dataBaseName}
		}else {
			log.Logf("Invalid policy:%s\n", plan.PolicyId)
			return ERR_POLICY_NOT_EXIST
		}
	}

	//Add to database
	return db.DbAdapter.CreatePlan(plan)
}

func Delete(id string, tenant string) error {
	return db.DbAdapter.DeletePlan(id, tenant)
}

//1. cannot update type
func Update(plan *Plan) error {
	if plan.Name != "" {
		m, err := regexp.MatchString("[[:alnum:]-_.]+", plan.Name)
		if !m || plan.Name == "all"{
			log.Logf("Invalid plan name[%s],err:", plan.Name,err) //cannot use all as name
			return ERR_INVALID_PLAN_NAME
		}
	}

	//TOkkDO check validation of tenant
	curPlan, error := db.DbAdapter.GetPlanByid(plan.Id.Hex(), plan.Tenant)
	if error != nil {
		log.Logf("Update plan failed, err: connot get the plan(%v).\n",error.Error())
		return error
	}

	if plan.Name == "" {
		plan.Name = curPlan.Name
	}
	if reflect.DeepEqual(plan.SourceConn, Connector{}) {
		plan.SourceConn = curPlan.SourceConn
	}
	if reflect.DeepEqual(plan.DestConn, Connector{}) {
		plan.DestConn = curPlan.DestConn
	}
	if reflect.DeepEqual(plan.Filt, Filter{}) {
		plan.Filt = curPlan.Filt
	}

	if plan.PolicyId != "" {
		if bson.IsObjectIdHex(plan.PolicyId) {
			plan.PolicyRef = mgo.DBRef{tblPolicy, plan.PolicyId, dataBaseName}
		}else {
			log.Logf("Invalid policy:%s\n", plan.PolicyId)
			return ERR_POLICY_NOT_EXIST
		}
	}else if curPlan.PolicyId != ""{
		plan.PolicyId = curPlan.PolicyId
		plan.PolicyRef = curPlan.PolicyRef
	}

	return db.DbAdapter.UpdatePlan(plan)
}

func Get(name string, tenant string) ([]Plan, error) {
	m, err := regexp.MatchString("[[:alnum:]-_.]*", name)
	if !m {
		log.Logf("Invalid plan name[%s],err:%v]n", name,err)
		return nil,ERR_INVALID_PLAN_NAME
	}

	return db.DbAdapter.GetPlan(name, tenant)
}

func getLocation(conn *Connector) (string, error){
	switch conn.StorType {
	case STOR_TYPE_OPENSDS:
		return conn.BucketName,nil
	default:
		log.Logf("Unsupport cnnector type:%v, return ERR_INNER_ERR\n", conn.StorType)
		return "",ERR_INNER_ERR
	}
}

func sendJob(req *datamover.RunJobRequest, mclient datamover.DatamoverService) error{
	ch := make(chan int)
	go func (req *datamover.RunJobRequest){
		ctx := context.Background()
		_, err := mclient.Runjob(ctx, req)
		if err != nil {
			log.Logf("Run job failed, err:%v\n", err)
			ch <- 1
		}else {
			log.Log("Run job succeed.")
			ch <- 0
		}
	}(req)

	select {
	case n := <-ch:
		log.Logf("Run job end, n=%d\n",n)
	case <- time.After(1800*time.Second):
		log.Log("Wait job timeout.")
	}

	return nil
}

func buildConn(reqConn *datamover.Connector, conn *_type.Connector) {
	if conn.StorType == STOR_TYPE_OPENSDS {
		reqConn.BucketName = conn.BucketName
	}else {
		for i := 0; i < len(conn.ConnConfig); i++ {
			reqConn.ConnConfig = append(reqConn.ConnConfig, &datamover.KV{Key:conn.ConnConfig[i].Key, Value:conn.ConnConfig[i].Value})
		}
	}
}

func Run(id string, tenant string, mclient datamover.DatamoverService) (bson.ObjectId,error) {
	//Get information from database
	plan,err := db.DbAdapter.GetPlanByid(id, tenant)
	if err != nil {
		return "",err
	}

	//scheduling must be mutual excluded among several schedulers
	//Get Lock
	ret := db.DbAdapter.LockSched(string(plan.Id.Hex()))
	for i := 0; i < 3; i++ {
		if ret == LockSuccess {
			//Make sure unlock before return
			defer db.DbAdapter.UnlockSched(string(plan.Id.Hex()))
			break
		}else if ret == LockBusy{
			return "",ERR_RUN_PLAN_BUSY
		} else {
			//Try to lock again, try three times at most
			ret = db.DbAdapter.LockSched(string(plan.Id.Hex()))
		}
	}

	//Get source location by source connector
	srcLocation,err1 := getLocation(&plan.SourceConn)
	if err1 != nil {
		return "",err1
	}

	//Get destination location by destination connector
	destLocation,err2 := getLocation(&plan.DestConn)
	if err2 != nil {
		return "",err2
	}

	ct := time.Now()
	//Create job
	job := Job{}
	//obId := bson.NewObjectId()
	//job.Id = jobId
	job.Type = plan.Type
	job.PlanId = string(plan.Id.Hex())
	job.PlanName = plan.Name
	job.SourceLocation = srcLocation
	job.DestLocation = destLocation
	job.CreateTime = ct
	job.Status = JOB_STATUS_QUEUEING
	//job.OverWrite = plan.OverWrite
	//job.RemainSource = plan.RemainSource

	//add job to database
	errno := db.DbAdapter.CreateJob(&job)
	if errno == nil {
		//TODO: change to send job to datamover by kafka
		//This way send job is the temporary
		req := datamover.RunJobRequest{Id:plan.Id.Hex(), OverWrite:plan.OverWrite, RemainSource:plan.RemainSource}
		srcConn := datamover.Connector{StorType:plan.SourceConn.StorType}
		buildConn(&srcConn, &plan.SourceConn)
		req.SourceConn = &srcConn
		destConn := datamover.Connector{StorType:plan.DestConn.StorType}
		buildConn(&destConn, &plan.DestConn)
		req.DestConn = &destConn
		go sendJob(&req, mclient)
	}else {
		log.Logf("Add job[id=%s,plan=%s,source_location=%s,dest_location=%s] to database failed.\n", string(job.Id.Hex()),
			job.PlanName, job.SourceLocation, job.DestLocation)
	}

	return job.Id, nil
}
