package plan

import (
	"regexp"
	"fmt"
	"github.com/opensds/go-panda/dataflow/pkg/db"
	"github.com/globalsign/mgo"
	. "github.com/opensds/go-panda/dataflow/pkg/type"
	"github.com/globalsign/mgo/bson"
	"time"
)

var dataBaseName = "test"
var tblConnector = "connector"
var tblPolicy = "policy"
/*func checkParam(plan *Plan) error {
	m, _ := regexp.MatchString("[[:alnum:]-_.]+", plan.Name)
	if !m {
		fmt.Printf("Invalid plan name:", plan.Name)
		return errors.New("Invalid plan name")
	}

	plan.IsSched = false //set to be false as default
	plan.SchedServer = "" // set to be null as default
	//plan.Type = "migration" //set to be migration as default
	plan.LastSchedTime = 0 //set to be 0 as default

	m, _ = regexp.MatchString("[[:alnum:]-_.]+", plan.SourceConnName)
	if !m {
		fmt.Printf("Invalid SourceConnName name name:", plan.SourceConnName)
		return errors.New("Invalid Source Connector Name")
	}
	m, _ = regexp.MatchString("[[:alnum:]-_.]+", plan.DestConnName)
	if !m {
		fmt.Printf("Invalid DestConnName name name:", plan.DestConnName)
		return errors.New("Invalid Destination Connector Name")
	}

	return nil
}*/

/*func Create(plan *Plan) ErrCode {
	//Check parameter validity
	m, err := regexp.MatchString("[[:alnum:]-_.]+", plan.Name)
	if !m {
		fmt.Printf("Invalid plan name[%s], err:%v", plan.Name, err)
		return ERR_INVALID_PLAN_NAME
	}
	if (plan.DestConnName == "" || plan.SourceConnName == ""){
		fmt.Println("Destination connector or source connector is not provided")
		return ERR_NO_DEST_SRC_CONN_PROVIDED
	}

	plan.Id = ""
	plan.IsSched = false //set to be false as default
	plan.SchedServer = "" // set to be null as default
	plan.LastSchedTime = 0 //set to be 0 as default

	//Check logic validity
	if plan.DestConnName != "" {
		destConn,err := connector.Get(plan.DestConnName, plan.Tenant)
		if err != ERR_OK {
			return ERR_DEST_CONN_NOT_EXIST
		} else {
			fmt.Printf("%v\n", destConn[0])
			plan.DestConnRef = mgo.DBRef{tblConnector,destConn[0].Id,dataBaseName}
		}
	}
	if plan.SourceConnName != "" {
		srcConn,err := connector.Get(plan.SourceConnName, plan.Tenant)
		if err != ERR_OK {
			return ERR_SRC_CONN_NOT_EXIST
		} else {
			plan.SourceConnRef = mgo.DBRef{tblConnector,srcConn[0].Id, dataBaseName}
		}
	}
	if plan.PolicyName != "" {
		pol,err := policy.Get(plan.PolicyName, plan.Tenant)
		if err != ERR_OK {
			return ERR_POLICY_NOT_EXIST
		} else {
			plan.PolicyRef = mgo.DBRef{tblPolicy, pol[0].Id, dataBaseName}
		}
	}

	//Add to database
	return db.DbAdapter.CreatePlan(plan)
}*/

func Create(plan *Plan) ErrCode {
	//Check parameter validity
	m, err := regexp.MatchString("[[:alnum:]-_.]+", plan.Name)
	if !m {
		fmt.Printf("Invalid plan name[%s], err:%v", plan.Name, err)
		return ERR_INVALID_PLAN_NAME
	}

	plan.Id = ""
	//plan.IsSched = false //set to be false as default
	//plan.SchedServer = "" // set to be null as default
	plan.LastSchedTime = 0 //set to be 0 as default

	//Check logic validity
	if plan.DestConnId != "" {
		if bson.IsObjectIdHex(plan.DestConnId) {
			plan.DestConnRef = mgo.DBRef{tblConnector,bson.ObjectIdHex(plan.DestConnId), dataBaseName}
		}else {
			fmt.Printf("Invalid destination connector:%s\n", plan.DestConnId)
			return ERR_NO_DEST_SRC_CONN_INVALID
		}
	}

	if plan.SourceConnId != "" {
		if bson.IsObjectIdHex(plan.SourceConnId) {
			plan.SourceConnRef = mgo.DBRef{tblConnector,bson.ObjectIdHex(plan.SourceConnId), dataBaseName}
		}else {
			fmt.Printf("Invalid destination connector:%s\n", plan.DestConnId)
			return ERR_NO_DEST_SRC_CONN_INVALID
		}
	}

	if plan.PolicyId != "" {
		if bson.IsObjectIdHex(plan.PolicyId) {
			plan.PolicyRef = mgo.DBRef{tblPolicy, bson.ObjectIdHex(plan.PolicyId), dataBaseName}
		}else {
			fmt.Printf("Invalid policy:%s\n", plan.PolicyId)
			return ERR_POLICY_NOT_EXIST
		}
	}

	//Add to database
	return db.DbAdapter.CreatePlan(plan)
}

func Delete(id string, tenant string) ErrCode {
	return db.DbAdapter.DeletePlan(id, tenant)
}

func Update(plan *Plan) ErrCode {
	m, err := regexp.MatchString("[[:alnum:]-_.]+", plan.Name)
	if !m {
		fmt.Printf("Invalid plan name[%s],err:", plan.Name,err)
		return ERR_INVALID_PLAN_NAME
	}
	//TO-DO check validation of tenant
	oldPlan, errcode := db.DbAdapter.GetPlanByid(plan.Id.Hex(), plan.Tenant)
	if errcode != ERR_OK {
		fmt.Printf("Update plan failed, err: connot get the plan(%v).\n",errcode)
		return errcode
	}

	plan.Id = oldPlan.Id
	//plan.IsSched = oldPlan.IsSched //set to be false as default
	//plan.SchedServer = oldPlan.SchedServer // set to be null as default
	plan.LastSchedTime = oldPlan.LastSchedTime //set to be 0 as default
	plan.PolicyRef = mgo.DBRef{"policy", plan.PolicyId, "test"}
	plan.DestConnRef = mgo.DBRef{"connector", plan.DestConnId, "test"}
	plan.SourceConnRef = mgo.DBRef{"connector", plan.SourceConnId, "test"}

	if plan.DestConnId != "" {
		if bson.IsObjectIdHex(plan.DestConnId) {
			plan.DestConnRef = mgo.DBRef{tblConnector,plan.DestConnId, dataBaseName}
		}else {
			fmt.Printf("Invalid destination connector:%s\n", plan.DestConnId)
			return ERR_NO_DEST_SRC_CONN_INVALID
		}
	}

	if plan.SourceConnId != "" {
		if bson.IsObjectIdHex(plan.SourceConnId) {
			plan.SourceConnRef = mgo.DBRef{tblConnector,plan.SourceConnId, dataBaseName}
		}else {
			fmt.Printf("Invalid destination connector:%s\n", plan.DestConnId)
			return ERR_NO_DEST_SRC_CONN_INVALID
		}
	}

	if plan.PolicyId != "" {
		if bson.IsObjectIdHex(plan.PolicyId) {
			plan.PolicyRef = mgo.DBRef{tblPolicy, plan.PolicyId, dataBaseName}
		}else {
			fmt.Printf("Invalid policy:%s\n", plan.PolicyId)
			return ERR_POLICY_NOT_EXIST
		}
	}

	return db.DbAdapter.UpdatePlan(plan)
}

func Get(name string, tenant string) ([]Plan, ErrCode) {
	m, err := regexp.MatchString("[[:alnum:]-_.]*", name)
	if !m {
		fmt.Printf("Invalid plan name[%s],err:%v]n", name,err)
		return nil,ERR_INVALID_PLAN_NAME
	}

	return db.DbAdapter.GetPlan(name, tenant)
}

func getSrcLocations(id string, tenant string) ([]string, string, ErrCode){
	locs := []string{}
	locs = append(locs, "test")
	srcBucket := "" //If source connector is not self-defined, srcBucket should not be null
	return locs,srcBucket,ERR_OK
}

func getDestLocation(id string, tenant string) (string, string, ErrCode) {
	destBucket := "" //If destination connector is self-defined, destBucket should not be null
	return "test",destBucket,ERR_OK
}

func Run(id string, tenant string) ErrCode {
	//Get information from database
	plan,err := db.DbAdapter.GetPlanByid(id, tenant)
	if err != ERR_OK {
		return err
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
			return ERR_RUN_PLAN_BUSY
		} else {
			//Try to lock again, try three times at most
			ret = db.DbAdapter.LockSched(string(plan.Id.Hex()))
		}
	}

	//Get source location by source connector, it could be more than one source location
	locs,srcBucket,err1 := getSrcLocations(plan.SourceConnId, plan.Tenant)
	if err1 != ERR_OK {
		return err1
	}

	//Get destination location by destination connector
	dest,destBucket,err2 := getDestLocation(plan.DestConnId, plan.Tenant)
	if err2 != ERR_OK {
		return err2
	}

	ct := time.Now()
	for i := 0; i < len(locs); i++ {
		//Create job
		job := Job{}
		//jobId := bson.NewObjectId()
		//job.Id = jobId
		job.Type = plan.Type
		job.PlanId = string(plan.Id.Hex())
		job.PlanName = plan.Name
		job.SourceLocation = locs[i]
		job.DestLocation = dest
		job.SourceBucket = srcBucket
		job.DestBucket = destBucket
		job.CreateTime = ct
		//add job to database

		errno := db.DbAdapter.CreateJob(&job)
		if errno == ERR_OK {
			//Send job to kafka
		}else {
			//TO-DO: It should be considered that what if some job add to database succeed, but the others failed.
			fmt.Printf("Add job[id=%s,plan=%s,source_location=%s,dest_location=%s] to database failed.\n", string(job.Id.Hex()),
				job.PlanName, job.SourceLocation, job.DestLocation)
		}
	}

	return ERR_OK
}
