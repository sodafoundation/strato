package mongo

import(
	"github.com/globalsign/mgo"
	. "github.com/opensds/go-panda/dataflow/pkg/type"
	"fmt"
	"github.com/globalsign/mgo/bson"
	"time"
)

var adap = &adapter{}
var DataBaseName = "test"
var lockColName = "mylock"
var lockManager = "manager"
var CollPolicy = "policy"
var CollConnector = "connector"
var CollJob	   = "job"
var CollPlan	= "plan"
var CollLock	= "mylock"
const (
	maxLockSec     = 5
)

type MyLock struct{
	LockObj string `bson:"lockobj"`
	LockTime time.Time `bson:"locktime"`
}

func Init(host string) *adapter {
	//fmt.Println("edps:", deps)
	session,err := mgo.Dial(host)
	if err != nil{
		panic(err)
	}
	//defer session.Close()

	session.SetMode(mgo.Monotonic, true)
	adap.s = session

	adap.userID = "unknown"

	return adap
}

func Exit() {
	adap.s.Close()
}

func TestClear() error{
	ss := adap.s.Copy()
	defer ss.Close()

	c := ss.DB(DataBaseName).C(CollPlan)
	err := c.Remove(bson.M{})
	if err != nil && err != mgo.ErrNotFound{
		fmt.Printf("clear plan err:%v\n", err)
		return err
	}

	c = ss.DB(DataBaseName).C(CollPolicy)
	err = c.Remove(bson.M{})
	if err != nil && err != mgo.ErrNotFound{
		fmt.Printf("clear policy err:%v\n", err)
		return err
	}

	c = ss.DB(DataBaseName).C(CollConnector)
	err = c.Remove(bson.M{})
	if err != nil && err != mgo.ErrNotFound{
		fmt.Printf("clear connector err:%v\n", err)
		return err
	}

	c = ss.DB(DataBaseName).C(CollLock)
	err = c.Remove(bson.M{})
	if err != nil && err != mgo.ErrNotFound{
		fmt.Printf("clear mylock err:%v\n", err)
		return err
	}

	return nil
}

type adapter struct {
	s *mgo.Session
	userID string
}

func lock(ss *mgo.Session, lockObj string, maxLockTime float64) int {
	c := ss.DB(DataBaseName).C(lockColName)
	lock := MyLock{lockObj, time.Now()}
	err := c.Insert(lock)
	if err == nil {
		fmt.Printf("Lock %s succeed.\n", lockObj)
		return LockSuccess
	}else {
		fmt.Printf("Try lock %s failed, err:%v.\n", lockObj,err)
		lk := MyLock{}
		err1 := c.Find(bson.M{"lockobj":lockObj}).One(&lk)
		if err1 == nil {
			fmt.Printf("%s is locked.\n", lockObj)
			now := time.Now()
			dur := now.Sub(lk.LockTime).Seconds()
			// If the obj is locked more than maxLockTime(in seconds) seconds, we consider the route call lock is crashed
			if dur > maxLockTime {
				fmt.Printf("%s is locked more than %f seconds, try to unlock it.\n", lockObj, dur)
				err2 := unlock(ss, lockObj)
				if err2 == LockSuccess { //If unlock success, try to lock again
					fmt.Printf("Try lock %s again.\n", lockObj)
					err3 := c.Insert(lock)
					if err3 == nil {
						fmt.Printf("Lock %s succeed.\n", lockObj)
						return LockSuccess
					}else {
						fmt.Printf("Lock %s failed.\n", lockObj)
					}
				}
			}else {
				fmt.Printf("%s is locked more less %f seconds, try to unlock it.\n", lockObj, dur)
				return LockBusy
			}
		}
	}

	return LockDbErr
}

func unlock(ss *mgo.Session, lockObj string) int {
	c := ss.DB(DataBaseName).C(lockColName)
	err := c.Remove(bson.M{"lockobj":lockObj})
	if err == nil {
		fmt.Printf("Unlock %s succeed.\n", lockObj)
		return LockSuccess
	}else {
		fmt.Printf("Unlock %s failed, err:%v.\n", lockObj,err)
		return LockDbErr
	}
}

func (ad *adapter) LockSched(planId string) int {
	ss := ad.s.Copy()
	defer ss.Close()

	return lock(ss, planId, 30) //One schedule is supposed to be finished in 30 seconds
}

func (ad *adapter) UnlockSched(planId string) int {
	ss := ad.s.Copy()
	defer ss.Close()

	return unlock(ss, planId)
}

func (ad *adapter) CreatePolicy(pol *Policy) ErrCode{
	ss := ad.s.Copy()
	defer ss.Close()
	c := ss.DB(DataBaseName).C(CollPolicy)
	err := c.Insert(&pol)
	if err != nil {
		fmt.Printf("Add policy to database failed, err:%v\n", err)
		return ERR_DB_ERR
	}

	return ERR_OK
}

func (ad *adapter) DeletePolicy(name string, tenant string) ErrCode{
	//Check if the policy exist or not
	ss := ad.s.Copy()
	defer ss.Close()

	//Get Lock
	ret := lock(ss, lockManager, maxLockSec)
	for i := 0; i < 3 && ret != LockSuccess; i++ {
		time.Sleep(time.Second * 1)
		ret = lock(ss, lockManager, maxLockSec)
	}
	if ret == LockSuccess {
		//Make sure unlock before return
		defer unlock(ss, lockManager)
	}else {
		return ERR_INNER_ERR
	}

	po := Policy{}
	c := ss.DB(DataBaseName).C(CollPolicy)
	err := c.Find(bson.M{"name":name, "tenant":tenant}).One(&po)
	if err == mgo.ErrNotFound{
		fmt.Println("Delete policy: the specified policy does not exist.")
		return ERR_POLICY_NOT_EXIST
	}else if err != nil {
		fmt.Println("Delete policy: DB error.")
		return ERR_DB_ERR
	}
	//Check if the policy is used by any plan, if it is used then it cannot be deleted
	cc := ss.DB(DataBaseName).C(CollPlan)
	count,erro := cc.Find(bson.M{"policy_ref:$ref":CollPolicy, "policy_ref:$id":po.Id, "policy_ref.$db":DataBaseName}).Count()
	if erro != nil {
		fmt.Printf("Delete policy failed, get related plan failed, err:%v.\n", erro)
		return ERR_DB_ERR
	}else if count > 0{
		fmt.Println("Delete policy failed, it is used by plan.")
		return ERR_IS_USED
	}

	//Delete it from database
	err = c.Remove(bson.M{"_id":po.Id})
	if err == mgo.ErrNotFound{
		fmt.Println("Delete policy: the specified policy does not exist.")
		return ERR_POLICY_NOT_EXIST
	}else if err != nil {
		fmt.Printf("Delete policy from database failed,err:%v.\n", err)
		return ERR_DB_ERR
	}
	return ERR_OK
}

func (ad *adapter) GetPolicy(name string, tenant string) ([]Policy, ErrCode){
	//var query mgo.Query;
	pols := []Policy{}
	ss := ad.s.Copy()
	defer ss.Close()
	c := ss.DB(DataBaseName).C(CollPolicy)
	if name == ""{//get all policies
		err := c.Find(bson.M{"tenant":tenant}).All(&pols)
		if err == mgo.ErrNotFound  || len(pols) == 0{
			fmt.Println("No policy found.")
			return nil, ERR_OK
		}else if err != nil {
			fmt.Println("Get policy from database failed.")
			return nil,ERR_DB_ERR
		}
	}else {//get specific policy
		err := c.Find(bson.M{"name":name, "tenant":tenant}).All(&pols)
		if err == mgo.ErrNotFound || len(pols) == 0{
			fmt.Println("Policy does not exist.")
			return nil,ERR_POLICY_NOT_EXIST
		}else if err != nil {
			fmt.Println("Get policy from database failed.")
			return nil,ERR_DB_ERR
		}
	}

	return pols,ERR_OK
}

func (ad *adapter)  GetPolicyById(id string, tenant string)(*Policy, ErrCode) {
	pol := Policy{}
	ss := ad.s.Copy()
	defer ss.Close()
	c := ss.DB(DataBaseName).C(CollPolicy)
	fmt.Printf("GetPolicyById: id=%s,tenant=%s\n", id, tenant)
	err := c.Find(bson.M{"_id":bson.ObjectIdHex(id), "tenant":tenant}).One(&pol)
	if err == mgo.ErrNotFound {
		fmt.Println("Plan does not exist.")
		return  nil,ERR_POLICY_NOT_EXIST
	}

	return &pol, ERR_OK
}

func (ad *adapter) UpdatePolicy(newPol *Policy) ErrCode{
	//Check if the policy exist or not
	ss := ad.s.Copy()
	defer ss.Close()
	/*pol := Policy{}
	err := c.Find(bson.M{"name":newPol.Name, "tenant":newPol.Tenant}).One(&pol)
	if err == mgo.ErrNotFound{
		fmt.Println("Update policy failed, err: the specified policy does not exist.")
		return ERR_POLICY_NOT_EXIST
	}else if err != nil {
		fmt.Printf("Update policy failed, err: %v.\n", err)
		return ERR_DB_ERR
	}*/

	//Get Lock
	ret := lock(ss, lockManager, maxLockSec)
	for i := 0; i < 3 && ret != LockSuccess; i++ {
		time.Sleep(time.Second * 1)
		ret = lock(ss, lockManager, maxLockSec)
	}
	if ret == LockSuccess {
		//Make sure unlock before return
		defer unlock(ss, lockManager)
	}else {
		return ERR_INNER_ERR
	}

	//Update database
	c := ss.DB(DataBaseName).C(CollPolicy)
	err := c.Update(bson.M{"_id":newPol.Id}, newPol)
	if err == mgo.ErrNotFound{
		fmt.Println("Update policy failed, err: the specified policy does not exist.")
		return ERR_POLICY_NOT_EXIST
	}else if err != nil {
		fmt.Printf("Update policy in database failed, err: %v.\n", err)
		return ERR_DB_ERR
	}
	return ERR_OK
}

func (ad *adapter)CreateConnector(conn *Connector) ErrCode{
	ss := ad.s.Copy()
	defer ss.Close()
	c := ss.DB(DataBaseName).C(CollConnector)
	err := c.Insert(&conn)
	if err != nil {
		fmt.Printf("Add connector into database failed, err:%v\n", err)
		return ERR_DB_ERR
	}

	return ERR_OK
}

func (ad *adapter)DeleteConnector(name string, tenant string) ErrCode{
	//Check if the connctor exist or not
	ss := ad.s.Copy()
	defer ss.Close()


	//Get Lock
	ret := lock(ss, lockManager, maxLockSec)
	for i := 0; i < 3 && ret != LockSuccess; i++ {
		time.Sleep(time.Second * 1)
		ret = lock(ss, lockManager, maxLockSec)
	}
	if ret == LockSuccess {
		//Make sure unlock before return
		defer unlock(ss, lockManager)
	}else {
		return ERR_INNER_ERR
	}

	conn := Connector{}
	c := ss.DB(DataBaseName).C(CollConnector)
	err := c.Find(bson.M{"name":name, "tenant":tenant}).One(&conn)
	if err == mgo.ErrNotFound{
		fmt.Println("Delete connector failed, err:the specified policy does not exist.")
		return ERR_CONN_NOT_EXIST
	}else if err != nil {
		fmt.Printf("Delete connector failed, err:%v.\n",err)
		return ERR_DB_ERR
	}

	//Check if the connector is used by any plan, if it is used then it cannot be deleted
	cc := ss.DB(DataBaseName).C(CollPlan)
	count,erro := cc.Find(bson.M{"src_conn_ref:$ref":CollConnector, "src_conn_ref:$id":conn.Id, "src_conn_ref.$db":DataBaseName}).Count()
	if erro != nil {
		fmt.Printf("Delete connector failed, get related plan failed, err:%v.\n", erro)
		return ERR_DB_ERR
	}else if count > 0{
		fmt.Println("Delete connector failed, it is used as source connector by plan.")
		return ERR_IS_USED
	}
	count1,erro1 := cc.Find(bson.M{"dest_conn_ref:$ref":CollConnector, "dest_conn_ref:$id":conn.Id, "dest_conn_ref.$db":DataBaseName}).Count()
	if erro1 != nil {
		fmt.Printf("Delete connector failed, get related plan failed, err:%v.\n", erro)
		return ERR_DB_ERR
	}else if count1 > 0{
		fmt.Println("Delete connector failed, it is used as destination connector by plan.")
		return ERR_IS_USED
	}

	//Delete it from database
	err = c.Remove(bson.M{"_id":conn.Id})
	if err == mgo.ErrNotFound{
		fmt.Printf("Delete connector from database failed,err:%v.\n", err)
		return ERR_CONN_NOT_EXIST
	}else if err != nil {
		fmt.Printf("Delete connector from database failed,err:%v.\n", err)
		return ERR_DB_ERR
	}

	return ERR_OK
}

func (ad *adapter)UpdateConnector(newConn *Connector) ErrCode{
	//Check if the policy exist or not
	ss := ad.s.Copy()
	defer ss.Close()
	/*conn := Connector{}
	err := c.Find(bson.M{"name":newConn.Name, "tenant":newConn.Tenant}).One(&conn)
	if err == mgo.ErrNotFound{
		fmt.Println("Delete connector: the specified connector does not exist.")
		return ERR_CONN_NOT_EXIST
	}else if err != nil {
		fmt.Printf("Delete connector failed, err:%v.\n", err)
		return ERR_DB_ERR
	}*/

	//Get Lock
	ret := lock(ss, lockManager, maxLockSec)
	for i := 0; i < 3 && ret != LockSuccess; i++ {
		time.Sleep(time.Second * 1)
		ret = lock(ss, lockManager, maxLockSec)
	}
	if ret == LockSuccess {
		//Make sure unlock before return
		defer unlock(ss, lockManager)
	}else {
		return ERR_INNER_ERR
	}

	//Update database
	c := ss.DB(DataBaseName).C(CollConnector)
	err := c.Update(bson.M{"_id":newConn.Id}, newConn)
	if err == mgo.ErrNotFound{
		fmt.Printf("Update conncetor in database failed, err: %v.\n", err)
		return ERR_CONN_NOT_EXIST
	}else if err != nil {
		fmt.Printf("Update conncetor in database failed, err: %v.\n", err)
		return ERR_DB_ERR
	}
	return ERR_OK
}

func (ad *adapter)GetConnector(name string, tenant string) ([]Connector, ErrCode){
	//var query mgo.Query;
	conns := []Connector{}
	ss := ad.s.Copy()
	defer ss.Close()
	c := ss.DB(DataBaseName).C(CollConnector)
	if name == ""{//get all Connectors
		err := c.Find(bson.M{"tenant":tenant}).All(&conns)
		if err == mgo.ErrNotFound || len(conns) == 0{
			fmt.Println("No connector found.")
			return  nil,ERR_OK
		}else if err != nil {
			fmt.Printf("Get connector from database failed,err:%v.\n", err)
			return  nil,ERR_DB_ERR
		}
	}else {//get specific Connector
		err := c.Find(bson.M{"name":name, "tenant":tenant}).All(&conns)
		if err == mgo.ErrNotFound || len(conns) == 0{
			fmt.Println("Connector not found.")
			return  nil,ERR_POLICY_NOT_EXIST
		}else if err != nil {
			fmt.Println("Get connector from database failed.")
			return  nil,ERR_DB_ERR
		}
	}
	return conns,ERR_OK
}

func (ad *adapter)GetConnectorById(id string, tenant string) (*Connector, ErrCode){
	conn := Connector{}
	ss := ad.s.Copy()
	defer ss.Close()
	c := ss.DB(DataBaseName).C(CollConnector)
	fmt.Printf("GetPlanByid: id=%s,tenant=%s\n", id, tenant)
	err := c.Find(bson.M{"_id":bson.ObjectIdHex(id), "tenant":tenant}).One(&conn)
	if err == mgo.ErrNotFound {
		fmt.Println("Plan does not exist.")
		return  nil,ERR_CONN_NOT_EXIST
	}

	return &conn, ERR_OK
}

func (ad *adapter)CreatePlan(plan *Plan) ErrCode{
	ss := ad.s.Copy()
	defer ss.Close()
	c := ss.DB(DataBaseName).C(CollPlan)
	err := c.Insert(&plan)

	//Get Lock, Create plan may depended on policy or connector
	ret := lock(ss, lockManager, maxLockSec)
	for i := 0; i < 3 && ret != LockSuccess; i++ {
		time.Sleep(time.Second * 1)
		ret = lock(ss, lockManager, maxLockSec)
	}
	if ret == LockSuccess {
		//Make sure unlock before return
		defer unlock(ss, lockManager)
	}else {
		return ERR_INNER_ERR
	}

	//Check if specific connector and policy exist or not
	errcode := checkPlanRelateObj(ss, plan)
	if  errcode != ERR_OK {
		return errcode
	}

	if err != nil {
		fmt.Printf("Insert plan into database failed, err:%v\n", err)
		return ERR_DB_ERR
	}

	return ERR_OK
}

func (ad *adapter)DeletePlan(name string, tenant string) ErrCode{
	//Check if the connctor exist or not
	ss := ad.s.Copy()
	defer ss.Close()

	//Get Lock
	ret := lock(ss, lockManager, maxLockSec)
	for i := 0; i < 3 && ret != LockSuccess; i++ {
		time.Sleep(time.Second * 1)
		ret = lock(ss, lockManager, maxLockSec)
	}
	if ret == LockSuccess {
		//Make sure unlock before return
		defer unlock(ss, lockManager)
	}else {
		return ERR_INNER_ERR
	}

	plan := Plan{}
	c := ss.DB(DataBaseName).C(CollPlan)
	err := c.Find(bson.M{"name":name, "tenant":tenant}).One(&plan)
	if err == mgo.ErrNotFound{
		fmt.Println("Delete plan failed, err:the specified plan does not exist.")
		return ERR_PLAN_NOT_EXIST
	}else if err != nil {
		fmt.Printf("Delete plan failed, err:%v.\n",err)
		return ERR_DB_ERR
	}

	//Delete it from database
	err = c.Remove(bson.M{"_id":plan.Id})
	if err == mgo.ErrNotFound{
		fmt.Println("Delete plan failed, err:the specified plan does not exist.")
		return ERR_PLAN_NOT_EXIST
	}else if err != nil {
		fmt.Printf("Delete plan from database failed,err:%v.\n", err)
		return ERR_DB_ERR
	}
	return ERR_OK
}

func checkPlanRelateObj(ss *mgo.Session, plan *Plan) ErrCode{
	if plan.PolicyId != ""{
		pol := Policy{}
		c := ss.DB(DataBaseName).C(CollPolicy)
		err := c.Find(bson.M{"_id":bson.ObjectIdHex(plan.PolicyId)}).One(&pol)
		if err != nil {
			fmt.Printf("Err: the specific policy[id:%s] not exist.\n", plan.PolicyId)
			return ERR_POLICY_NOT_EXIST
		}
	}
	if plan.SourceConnId != "" {
		cc := ss.DB(DataBaseName).C(CollConnector)
		conn := Connector{}
		err := cc.Find(bson.M{"_id":bson.ObjectIdHex(plan.SourceConnId)}).One(&conn)
		if err != nil {
			fmt.Printf("Err: the specific source connector[id:%s] not exist.\n", plan.SourceConnId)
			return ERR_SRC_CONN_NOT_EXIST
		}
	}

	if plan.SourceConnId != "" {
		cc := ss.DB(DataBaseName).C(CollConnector)
		conn := Connector{}
		err := cc.Find(bson.M{"_id":bson.ObjectIdHex(plan.DestConnId)}).One(&conn)
		if err != nil {
			fmt.Printf("Err: the specific destination connector[id:%s] not exist.\n", plan.DestConnId)
			return ERR_DEST_CONN_NOT_EXIST
		}
	}

	return ERR_OK
}

func (ad *adapter)UpdatePlan(plan *Plan) ErrCode{
	//Check if the policy exist or not
	ss := ad.s.Copy()
	defer ss.Close()

	//Get Lock
	ret := lock(ss, lockManager, maxLockSec)
	for i := 0; i < 3 && ret != LockSuccess; i++ {
		time.Sleep(time.Second * 1)
		ret = lock(ss, lockManager, maxLockSec)
	}
	if ret == LockSuccess {
		//Make sure unlock before return
		defer unlock(ss, lockManager)
	}else {
		return ERR_INNER_ERR
	}

	//Check if specific connector and policy exist or not
	errcode := checkPlanRelateObj(ss, plan)
	if  errcode != ERR_OK {
		return errcode
	}

	//Update database
	c := ss.DB(DataBaseName).C(CollPlan)
	err := c.Update(bson.M{"_id":plan.Id}, plan)
	if err == mgo.ErrNotFound{
		fmt.Println("Update plan: the specified plan does not exist.")
		return ERR_PLAN_NOT_EXIST
	}else if err != nil {
		fmt.Printf("Update plan in database failed, err: %v.\n", err)
		return ERR_DB_ERR
	}
	return ERR_OK
}

func (ad *adapter)GetPlan(name string, tenant string) ([]Plan, ErrCode) {
	//var query mgo.Query;
	plans := []Plan{}
	ss := ad.s.Copy()
	defer ss.Close()
	c := ss.DB(DataBaseName).C(CollPlan)
	fmt.Printf("name:%s, tenatn:%s\n", name, tenant)
	if name == ""{//get all Connectors
		err := c.Find(bson.M{"tenant":tenant}).All(&plans)
		if err == mgo.ErrNotFound || len(plans) == 0{
			fmt.Println("No plan found.")
			return  nil,ERR_OK
		}else if err != nil {
			fmt.Printf("Get plan from database failed,err:%v.\n",err)
			return  nil,ERR_DB_ERR
		}
	}else {//get specific Connector
		err := c.Find(bson.M{"name":name, "tenant":tenant}).All(&plans)
		if err == mgo.ErrNotFound || len(plans) == 0{
			fmt.Println("Plan does not exist.")
			return  nil,ERR_PLAN_NOT_EXIST
		}else if err != nil {
			fmt.Printf("Get plan from database failed,err:%v.",err)
			return  nil,ERR_DB_ERR
		}
	}

	//Get the name of related policy and connectors
	for i := 0; i < len(plans); i++ {
		var pol Policy
		var conn1,conn2 Connector
		if plans[i].SourceConnRef.Id != nil {
			err := ss.DB(DataBaseName).FindRef(&plans[i].SourceConnRef).One(&conn1)
			if err != nil {
				fmt.Printf("Get SourceConnRef failed,err:%v.",err)
				return  nil,ERR_DB_ERR
			}else{
				plans[i].SourceConnName = conn1.Name
				//plans[i].SourceConnId = string(conn1.Id.Hex())
			}
		}

		if plans[i].DestConnRef.Id != nil {
			err := ss.DB(DataBaseName).FindRef(&plans[i].DestConnRef).One(&conn2)
			if err != nil {
				fmt.Printf("Get DestConnRef failed,err:%v.",err)
				return  nil,ERR_DB_ERR
			}else{
				plans[i].DestConnName = conn2.Name
				//plans[i].DestConnId = string(conn2.Id.Hex())
			}
		}

		if plans[i].PolicyRef.Id != nil {
			err := ss.DB(DataBaseName).FindRef(&plans[i].PolicyRef).One(&pol)
			if err != nil {
				fmt.Printf("Get PolicyRef failed,err:%v.",err)
				return  nil,ERR_DB_ERR
			}else{
				plans[i].PolicyName = pol.Name
				//plans[i].PolicyId = string(pol.Id.Hex())
			}
		}
	}

	return plans,ERR_OK
}

func (ad *adapter)GetPlanByid(id string, tenant string) (*Plan, ErrCode) {
	plan := Plan{}
	ss := ad.s.Copy()
	defer ss.Close()
	c := ss.DB(DataBaseName).C(CollPlan)
	fmt.Printf("GetPlanByid: id=%s,tenant=%s\n", id, tenant)
	err := c.Find(bson.M{"_id":bson.ObjectIdHex(id), "tenant":tenant}).One(&plan)
	if err == mgo.ErrNotFound {
		fmt.Println("Plan does not exist.")
		return  nil,ERR_PLAN_NOT_EXIST
	}

	//Get the name of related policy and connectors
	var pol Policy
	var conn1,conn2 Connector
	if plan.SourceConnRef.Id != nil {
		err1 := ss.DB(DataBaseName).FindRef(&plan.SourceConnRef).One(&conn1)
		if err1 != nil {
			fmt.Printf("Get SourceConnRef failed,err:%v.",err)
			return  nil,ERR_DB_ERR
		}else{
			plan.SourceConnName = conn1.Name
			//plans[i].SourceConnId = string(conn1.Id.Hex())
		}
	}

	if plan.DestConnRef.Id != nil {
		err := ss.DB(DataBaseName).FindRef(&plan.DestConnRef).One(&conn2)
		if err != nil {
			fmt.Printf("Get DestConnRef failed,err:%v.",err)
			return  nil,ERR_DB_ERR
		}else{
			plan.DestConnName = conn2.Name
			//plans[i].DestConnId = string(conn2.Id.Hex())
		}
	}

	if plan.PolicyRef.Id != nil {
		err := ss.DB(DataBaseName).FindRef(&plan.PolicyRef).One(&pol)
		if err != nil {
			fmt.Printf("Get PolicyRef failed,err:%v.",err)
			return  nil,ERR_DB_ERR
		}else{
			plan.PolicyName = pol.Name
			//plans[i].PolicyId = string(pol.Id.Hex())
		}
	}

	return &plan, ERR_OK
}

func (ad *adapter) CreateJob(job *Job) ErrCode {
	ss := ad.s.Copy()
	defer ss.Close()

	c := ss.DB(DataBaseName).C(CollJob)
	err := c.Insert(&job)
	for i := 0; i < 3; i++ {
		if mgo.IsDup(err) {
			fmt.Printf("Add job into database failed, duplicate index:%s\n", string(job.Id.Hex()))
			jobId := bson.NewObjectId()
			job.Id = jobId
			err = c.Insert(&job)
		}else {
			if err == nil {
				fmt.Printf("Add job into database succeed, job id:%v\n", string(job.Id.Hex()))
				return ERR_OK
			}else {
				fmt.Printf("Add job into database failed, err:%v\n", err)
				return ERR_DB_ERR
			}
		}
	}

	fmt.Println("Add job failed, objectid duplicate too much times.")
	return ERR_DB_ERR
}