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

package mongo

import (
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/micro/go-log"
	. "github.com/opensds/multi-cloud/api/pkg/filters/context"
	. "github.com/opensds/multi-cloud/dataflow/pkg/model"
)

var adap = &adapter{}
var DataBaseName = "multi-cloud"
var lockColName = "mylock"
var lockManager = "manager"
var CollPolicy = "policy"
var CollConnector = "connector"
var CollJob = "job"
var CollPlan = "plan"

const (
	maxLockSec = 5
)

type MyLock struct {
	LockObj  string    `bson:"lockobj"`
	LockTime time.Time `bson:"locktime"`
}

func Init(host string) *adapter {
	//log.Log("edps:", deps)
	session, err := mgo.Dial(host)
	if err != nil {
		log.Log("Connect database failed.")
		panic(err)
	}

	session.SetMode(mgo.Monotonic, true)
	adap.s = session
	adap.userID = "unknown"

	lockColl := session.DB(DataBaseName).C(lockColName)
	//Check if index is realdy set.
	indxs, err := lockColl.Indexes()
	if err != nil || len(indxs) == 0 {
		//Set unique index of the collection of lockColName
		index := mgo.Index{
			Key:      []string{"lockobj"}, //index key
			Unique:   true,                //Prevent two documents from having the same index key
			DropDups: false,               //Drop documents with the same index key as a previously indexed one.
			// Invalid when Unique equals true.
			Background: true, //If Background is true, other connections will be allowed to proceed
			// using the collection without the index while it's being built.
		}
		if err := lockColl.EnsureIndex(index); err != nil {
			log.Fatalf("Create unique index of %s faild:%v.\n", lockColName, err)
		}
	}

	return adap
}

func isAdmin(ctx *Context) bool {
	return ctx.IsAdmin
}

func Exit() {
	adap.s.Close()
}

func TestClear() error {
	ss := adap.s.Copy()
	defer ss.Close()

	c := ss.DB(DataBaseName).C(CollPlan)
	err := c.Remove(bson.M{})
	if err != nil && err != mgo.ErrNotFound {
		log.Logf("clear plan err:%v\n", err)
		return err
	}

	c = ss.DB(DataBaseName).C(CollPolicy)
	err = c.Remove(bson.M{})
	if err != nil && err != mgo.ErrNotFound {
		log.Logf("clear policy err:%v\n", err)
		return err
	}

	c = ss.DB(DataBaseName).C(CollConnector)
	err = c.Remove(bson.M{})
	if err != nil && err != mgo.ErrNotFound {
		log.Logf("clear connector err:%v\n", err)
		return err
	}

	c = ss.DB(DataBaseName).C(lockColName)
	err = c.Remove(bson.M{})
	if err != nil && err != mgo.ErrNotFound {
		log.Logf("clear mylock err:%v\n", err)
		return err
	}

	return nil
}

type adapter struct {
	s      *mgo.Session
	userID string
}

func lock(ss *mgo.Session, lockObj string, maxLockTime float64) int {
	c := ss.DB(DataBaseName).C(lockColName)
	lock := MyLock{lockObj, time.Now()}
	err := c.Insert(lock)
	if err == nil {
		log.Logf("Lock %s succeed.\n", lockObj)
		return LockSuccess
	} else {
		log.Logf("Try lock %s failed, err:%v.\n", lockObj, err)
		lk := MyLock{}
		err1 := c.Find(bson.M{"lockobj": lockObj}).One(&lk)
		if err1 == nil {
			log.Logf("%s is locked.\n", lockObj)
			now := time.Now()
			dur := now.Sub(lk.LockTime).Seconds()
			// If the obj is locked more than maxLockTime(in seconds) seconds, we consider the route call lock is crashed
			if dur > maxLockTime {
				log.Logf("%s is locked more than %f seconds, try to unlock it.\n", lockObj, dur)
				err2 := unlock(ss, lockObj)
				if err2 == LockSuccess { //If unlock success, try to lock again
					log.Logf("Try lock %s again.\n", lockObj)
					err3 := c.Insert(lock)
					if err3 == nil {
						log.Logf("Lock %s succeed.\n", lockObj)
						return LockSuccess
					} else {
						log.Logf("Lock %s failed.\n", lockObj)
					}
				}
			} else {
				log.Logf("%s is locked more less %f seconds, try to unlock it.\n", lockObj, dur)
				return LockBusy
			}
		}
	}

	return LockDbErr
}

func unlock(ss *mgo.Session, lockObj string) int {
	c := ss.DB(DataBaseName).C(lockColName)
	err := c.Remove(bson.M{"lockobj": lockObj})
	if err == nil {
		log.Logf("Unlock %s succeed.\n", lockObj)
		return LockSuccess
	} else {
		log.Logf("Unlock %s failed, err:%v.\n", lockObj, err)
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

func (ad *adapter) CreatePolicy(ctx *Context, pol *Policy) (*Policy, error) {
	pol.Tenant = ctx.TenantId
	pol.Id = bson.NewObjectId()
	ss := ad.s.Copy()
	defer ss.Close()
	c := ss.DB(DataBaseName).C(CollPolicy)
	err := c.Insert(&pol)
	if err != nil {
		log.Logf("Add policy to database failed, err:%v\n", err)
		return nil, ERR_DB_ERR
	}

	return pol, nil
}

func (ad *adapter) DeletePolicy(ctx *Context, id string) error {
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
	} else {
		return ERR_INNER_ERR
	}

	po := Policy{}
	c := ss.DB(DataBaseName).C(CollPolicy)
	err := c.Find(bson.M{"_id": bson.ObjectIdHex(id), "tenant": ctx.TenantId}).One(&po)
	if err == mgo.ErrNotFound {
		log.Log("Delete policy: the specified policy does not exist.")
		return ERR_POLICY_NOT_EXIST
	} else if err != nil {
		log.Log("Delete policy: DB error.")
		return ERR_DB_ERR
	}
	//Check if the policy is used by any plan, if it is used then it cannot be deleted
	cc := ss.DB(DataBaseName).C(CollPlan)
	count, erro := cc.Find(bson.M{"policy_ref:$ref": CollPolicy, "policy_ref:$id": po.Id, "policy_ref.$db": DataBaseName}).Count()
	if erro != nil {
		log.Logf("Delete policy failed, get related plan failed, err:%v.\n", erro)
		return ERR_DB_ERR
	} else if count > 0 {
		log.Log("Delete policy failed, it is used by plan.")
		return ERR_IS_USED_BY_PLAN
	}

	//Delete it from database
	err = c.Remove(bson.M{"_id": po.Id})
	if err == mgo.ErrNotFound {
		log.Log("Delete policy: the specified policy does not exist.")
		return ERR_POLICY_NOT_EXIST
	} else if err != nil {
		log.Logf("Delete policy from database failed,err:%v.\n", err)
		return ERR_DB_ERR
	}
	return nil
}

func (ad *adapter) ListPolicy(ctx *Context) ([]Policy, error) {
	//var query mgo.Query;
	pols := []Policy{}
	ss := ad.s.Copy()
	defer ss.Close()
	c := ss.DB(DataBaseName).C(CollPolicy)
	m := bson.M{}
	if !isAdmin(ctx) {
		m["tenant"] = ctx.TenantId
	}
	err := c.Find(m).All(&pols)
	if err == mgo.ErrNotFound || len(pols) == 0 {
		log.Log("No policy found.")
		return nil, nil
	} else if err != nil {
		log.Log("Get policy from database failed.")
		return nil, ERR_DB_ERR
	}
	return pols, nil
}

func (ad *adapter) GetPolicy(ctx *Context, id string) (*Policy, error) {
	pol := Policy{}
	ss := ad.s.Copy()
	defer ss.Close()
	c := ss.DB(DataBaseName).C(CollPolicy)
	log.Logf("GetPolicy: id=%s,tenant=%s\n", id, ctx.TenantId)
	err := c.Find(bson.M{"_id": bson.ObjectIdHex(id), "tenant": ctx.TenantId}).One(&pol)
	if err == mgo.ErrNotFound {
		log.Log("Plan does not exist.")
		return nil, ERR_POLICY_NOT_EXIST
	}

	return &pol, nil
}

func (ad *adapter) UpdatePolicy(ctx *Context, newPol *Policy) (*Policy, error) {
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
	} else {
		return nil, ERR_INNER_ERR
	}

	//Update database
	c := ss.DB(DataBaseName).C(CollPolicy)
	err := c.Update(bson.M{"_id": newPol.Id}, newPol)
	if err == mgo.ErrNotFound {
		//log.Log("Update policy failed, err: the specified policy does not exist.")
		log.Logf("Update policy in database failed, err: %v.", err)
		return nil, ERR_POLICY_NOT_EXIST
	} else if err != nil {
		//log.Logf("Update policy in database failed, err: %v.\n", err)
		log.Logf("Update policy in database failed, err: %v.", err)
		return nil, ERR_DB_ERR
	}

	log.Log("Update policy succeefully.")
	return newPol, nil
}

func (ad *adapter) CreatePlan(ctx *Context, plan *Plan) (*Plan, error) {
	plan.TenantId = ctx.TenantId
	ss := ad.s.Copy()
	defer ss.Close()
	c := ss.DB(DataBaseName).C(CollPlan)
	//Get Lock, Create plan may depended on policy or connector
	ret := lock(ss, lockManager, maxLockSec)
	for i := 0; i < 3 && ret != LockSuccess; i++ {
		time.Sleep(time.Second * 1)
		ret = lock(ss, lockManager, maxLockSec)
	}
	if ret == LockSuccess {
		//Make sure unlock before return
		defer unlock(ss, lockManager)
	} else {
		return nil, ERR_INNER_ERR
	}

	//Check if specific connector and policy exist or not
	err := checkPlanRelateObj(ss, plan)
	if err != nil {
		return nil, err
	}

	if plan.PolicyId != "" {
		if bson.IsObjectIdHex(plan.PolicyId) {
			plan.PolicyRef = mgo.DBRef{CollPolicy, bson.ObjectIdHex(plan.PolicyId), DataBaseName}
		} else {
			log.Logf("Invalid policy:%s\n", plan.PolicyId)
			return nil, ERR_POLICY_NOT_EXIST
		}
	}

	//Create plan id
	for i := 0; i < 3; i++ {
		plan.Id = bson.NewObjectId()
		err = c.Insert(plan)
		if err != nil && mgo.IsDup(err) {
			log.Logf("Add plan into database failed, duplicate id:%s\n", string(plan.Id.Hex()))
			continue
		}
		break
	}

	return plan, err
}

func (ad *adapter) DeletePlan(ctx *Context, id string) error {
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
	} else {
		return ERR_INNER_ERR
	}

	p := Plan{}
	c := ss.DB(DataBaseName).C(CollPlan)
	err := c.Find(bson.M{"_id": bson.ObjectIdHex(id), "tenantId": ctx.TenantId}).One(&p)
	if err == mgo.ErrNotFound {
		log.Log("Delete plan failed, err:the specified p does not exist.")
		return ERR_PLAN_NOT_EXIST
	} else if err != nil {
		log.Logf("Delete plan failed, err:%v.\n", err)
		return ERR_DB_ERR
	}

	//Delete it from database
	err = c.Remove(bson.M{"_id": p.Id})
	if err == mgo.ErrNotFound {
		log.Log("Delete plan failed, err:the specified p does not exist.")
		return ERR_PLAN_NOT_EXIST
	} else if err != nil {
		log.Logf("Delete plan from database failed,err:%v.\n", err)
		return ERR_DB_ERR
	}

	log.Log("Delete plan successfully.")
	return nil
}

func checkPlanRelateObj(ss *mgo.Session, plan *Plan) error {
	if plan.PolicyId != "" {
		pol := Policy{}
		c := ss.DB(DataBaseName).C(CollPolicy)
		err := c.Find(bson.M{"_id": bson.ObjectIdHex(plan.PolicyId)}).One(&pol)
		if err != nil {
			log.Logf("Err: the specific policy[id:%s] not exist.\n", plan.PolicyId)
			return ERR_POLICY_NOT_EXIST
		}
	}

	return nil
}

func (ad *adapter) UpdatePlan(ctx *Context, plan *Plan) (*Plan, error) {
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
	} else {
		return nil, ERR_INNER_ERR
	}

	//Check if specific connector and policy exist or not
	err := checkPlanRelateObj(ss, plan)
	if err != nil {
		return nil, err
	}

	if plan.PolicyId != "" {
		if bson.IsObjectIdHex(plan.PolicyId) {
			plan.PolicyRef = mgo.DBRef{CollPolicy, bson.ObjectIdHex(plan.PolicyId), DataBaseName}
		} else {
			log.Logf("Invalid policy:%s\n", plan.PolicyId)
			return nil, ERR_POLICY_NOT_EXIST
		}
	} else {
		plan.PolicyRef = mgo.DBRef{}
	}

	//Update database
	c := ss.DB(DataBaseName).C(CollPlan)
	err = c.Update(bson.M{"_id": plan.Id}, plan)
	if err == mgo.ErrNotFound {
		log.Logf("Update plan: the specified plan[id=%v] does not exist.", plan.Id)
		return nil, ERR_PLAN_NOT_EXIST
	} else if err != nil {
		log.Logf("Update plan in database failed, err: %v.\n", err)
		return nil, ERR_DB_ERR
	}
	return plan, nil
}

func (ad *adapter) ListPlan(ctx *Context, limit int, offset int, filter interface{}) ([]Plan, error) {
	//m := bson.M{}
	//if !isAdmin(ctx) {
	//	//m["tenant"] = ctx.TenantId
	//	filter.(map[string]string)["tenant"] = ctx.TenantId
	//}

	return ad.doListPlan(ctx, limit, offset, filter)
}

func (ad *adapter) doListPlan(ctx *Context, limit int, offset int, filter interface{}) ([]Plan, error) {
	//var query mgo.Query;
	plans := []Plan{}
	ss := ad.s.Copy()
	defer ss.Close()
	c := ss.DB(DataBaseName).C(CollPlan)

	log.Logf("Listplan filter:%v\n", filter)

	//Search plan by bucket
	var err error = nil
	if filter != nil {
		filt, ok := filter.(map[string]string)
		if !ok {
			log.Log("Reflect filter failed.")
			return nil, ERR_INNER_ERR
		}
		if filt["bucketname"] == "" {
			err = c.Find(filter).Skip(offset).Limit(limit).All(&plans)
		} else {
			query := []bson.M{}
			query = append(query, bson.M{"srcConn.bucketName":filt["bucketname"]})
			query = append(query, bson.M{"destConn.bucketName":filt["bucketname"]})
			err = c.Find(bson.M{"$or":query}).Skip(offset).Limit(limit).All(&plans)
		}
	}

	//err := c.Find(filter).Skip(offset).Limit(limit).All(&plans)
	if err == mgo.ErrNotFound || len(plans) == 0 {
		log.Log("No plan found.")
		return nil, nil
	} else if err != nil {
		log.Logf("Get plan from database failed,err:%v.\n", err)
		return nil, ERR_DB_ERR
	}

	//Get the name of related policy and connectors
	for i := 0; i < len(plans); i++ {
		var pol Policy
		if plans[i].PolicyId != "" {
			log.Logf("PolicyRef:%+v\n", plans[i].PolicyRef)
			err := ss.DB(DataBaseName).FindRef(&plans[i].PolicyRef).One(&pol)
			if err != nil {
				log.Logf("Get PolicyRef failed,err:%v.\n", err)
				return nil, ERR_DB_ERR
			} else {
				plans[i].PolicyName = pol.Name
				//plans[i].PolicyId = string(pol.Id.Hex())
			}
		}
	}

	return plans, nil
}

func (ad *adapter) GetPlan(ctx *Context, id string) (*Plan, error) {
	p := Plan{}
	ss := ad.s.Copy()
	defer ss.Close()
	c := ss.DB(DataBaseName).C(CollPlan)
	log.Logf("GetPlan: id=%s,tenantId=%s\n", id, ctx.TenantId)
	err := c.Find(bson.M{"_id": bson.ObjectIdHex(id), "tenantId": ctx.TenantId}).One(&p)
	if err == mgo.ErrNotFound {
		log.Log("Plan does not exist.")
		return nil, ERR_PLAN_NOT_EXIST
	}

	//Get the name of related policy and connectors
	var pol Policy
	if p.PolicyId != "" {
		err := ss.DB(DataBaseName).FindRef(&p.PolicyRef).One(&pol)
		if err != nil {
			log.Logf("Get PolicyRef failed,err:%v.\n", err)
			return nil, ERR_DB_ERR
		} else {
			p.PolicyName = pol.Name
			//plans[i].PolicyId = string(pol.Id.Hex())
		}
	}
	return &p, nil
}

func (ad *adapter) GetPlanByPolicy(ctx *Context, policyId string, limit int, offset int) ([]Plan, error) {
	log.Logf("GetPlanByPolicy: policyId=%s,tenantId=%s\n", policyId, ctx.TenantId)
	m := bson.M{"policyId": policyId}
	if !isAdmin(ctx) {
		m["tenantId"] = ctx.TenantId
	}
	return ad.doListPlan(ctx, limit, offset, m)
}

func (ad *adapter) CreateJob(ctx *Context, job *Job) (*Job, error) {
	job.Tenant = ctx.TenantId
	ss := ad.s.Copy()
	defer ss.Close()

	c := ss.DB(DataBaseName).C(CollJob)
	var err error
	i := 0
	for ; i < 3; i++ {
		job.Id = bson.NewObjectId()
		err = c.Insert(&job)
		if err != nil && mgo.IsDup(err) {
			log.Logf("Add job into database failed, duplicate id:%s\n", string(job.Id.Hex()))
			continue
		}
		break
	}
	if i == 3 {
		log.Log("Add job to database failed too much times.")
	}

	return job, err
}

func (ad *adapter) GetJob(ctx *Context, id string) (*Job, error) {
	//var query mgo.Query;
	job := Job{}
	ss := ad.s.Copy()
	defer ss.Close()
	c := ss.DB(DataBaseName).C(CollJob)

	err := c.Find(bson.M{"_id": bson.ObjectIdHex(id), "tenant": ctx.TenantId}).One(&job)
	if err == mgo.ErrNotFound {
		log.Log("Job does not exist.")
		return nil, ERR_JOB_NOT_EXIST
	}
	return &job, nil
}

func (ad *adapter) ListJob(ctx *Context, limit int, offset int, filter interface{}) ([]Job, error) {
	//var query mgo.Query;
	jobs := []Job{}
	ss := ad.s.Copy()
	defer ss.Close()
	c := ss.DB(DataBaseName).C(CollJob)
	//m := bson.M{}
	//if !isAdmin(ctx) {
	//	m["tenant"] = ctx.TenantId
	//}
	err := c.Find(filter).Skip(offset).Limit(limit).All(&jobs)
	if err == mgo.ErrNotFound || len(jobs) == 0 {
		log.Log("No jobs found.")
		return nil, nil
	} else if err != nil {
		log.Logf("Get jobs from database failed,err:%v.\n", err)
		return nil, ERR_DB_ERR
	}
	return jobs, nil
}
