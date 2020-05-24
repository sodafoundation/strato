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

package mongo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/micro/go-micro/metadata"
	"github.com/opensds/multi-cloud/api/pkg/common"
	. "github.com/opensds/multi-cloud/dataflow/pkg/model"
	log "github.com/sirupsen/logrus"
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

func setIndex(session *mgo.Session, colName string, keys []string, unique bool, dropDups bool, backgroudn bool) {
	coll := session.DB(DataBaseName).C(colName)
	log.Infof("Set unique index of %+v for %s.\n", keys, colName)
	//Check if index is already set, if not set it.
	var exist bool = true
	indxs, err := coll.Indexes()
	if err == nil {
		for _, indx := range indxs {
			for k, v := range indx.Key {
				if v != keys[k] {
					exist = false
					break
				}
			}
		}
		if exist == true {
			log.Info("index already exist")
			return
		}
	}

	index := mgo.Index{
		Key:      keys,     //index key
		Unique:   unique,   //Prevent two documents from having the same index key
		DropDups: dropDups, //Drop documents with the same index key as a previously indexed one.
		// Invalid when Unique equals true.
		Background: backgroudn, //If Background is true, other connections will be allowed to proceed
		// using the collection without the index while it's being built.
	}
	if err := coll.EnsureIndex(index); err != nil {
		log.Fatalf("create unique index failed: %+v.\n", err)
	}
}

func Init(host string) *adapter {
	//log.Info("edps:", deps)
	session, err := mgo.Dial(host)
	if err != nil {
		log.Info("Connect database failed.")
		panic(err)
	}

	session.SetMode(mgo.Monotonic, true)
	adap.s = session
	adap.userID = "unknown"

	setIndex(session, lockColName, []string{"lockobj"}, true, false, false)
	setIndex(session, CollPlan, []string{"name", "tenantId"}, true, false, false)

	return adap
}

func UpdateFilter(m bson.M, filter map[string]string) error {
	for k, v := range filter {
		m[k] = interface{}(v)
	}
	return nil
}

func UpdateContextFilter(ctx context.Context, m bson.M) error {
	// if context is admin, no need filter by tenantId.
	md, ok := metadata.FromContext(ctx)
	if !ok {
		log.Error("get context failed")
		return errors.New("get context failed")
	}

	isAdmin, _ := md[common.CTX_KEY_IS_ADMIN]
	if isAdmin != common.CTX_VAL_TRUE {
		tenantId, ok := md[common.CTX_KEY_TENANT_ID]
		if !ok {
			log.Error("get tenantid failed")
			return errors.New("get tenantid failed")
		}
		m["tenantId"] = tenantId
	}

	return nil
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
		log.Errorf("clear plan err:%v\n", err)
		return err
	}

	c = ss.DB(DataBaseName).C(CollPolicy)
	err = c.Remove(bson.M{})
	if err != nil && err != mgo.ErrNotFound {
		log.Errorf("clear policy err:%v\n", err)
		return err
	}

	c = ss.DB(DataBaseName).C(CollConnector)
	err = c.Remove(bson.M{})
	if err != nil && err != mgo.ErrNotFound {
		log.Errorf("clear connector err:%v\n", err)
		return err
	}

	c = ss.DB(DataBaseName).C(lockColName)
	err = c.Remove(bson.M{})
	if err != nil && err != mgo.ErrNotFound {
		log.Errorf("clear mylock err:%v\n", err)
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
		log.Infof("Lock %s succeed.\n", lockObj)
		return LockSuccess
	} else {
		log.Errorf("Try lock %s failed, err:%v.\n", lockObj, err)
		lk := MyLock{}
		err1 := c.Find(bson.M{"lockobj": lockObj}).One(&lk)
		if err1 == nil {
			log.Infof("%s is locked.\n", lockObj)
			now := time.Now()
			dur := now.Sub(lk.LockTime).Seconds()
			// If the obj is locked more than maxLockTime(in seconds) seconds, we consider the route call lock is crashed
			if dur > maxLockTime {
				log.Infof("%s is locked more than %f seconds, try to unlock it.\n", lockObj, dur)
				err2 := unlock(ss, lockObj)
				if err2 == LockSuccess { //If unlock success, try to lock again
					log.Infof("Try lock %s again.\n", lockObj)
					err3 := c.Insert(lock)
					if err3 == nil {
						log.Infof("Lock %s succeed.\n", lockObj)
						return LockSuccess
					} else {
						log.Errorf("Lock %s failed.\n", lockObj)
					}
				}
			} else {
				log.Infof("%s is locked more less %f seconds, try to unlock it.\n", lockObj, dur)
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
		log.Infof("Unlock %s succeed.\n", lockObj)
		return LockSuccess
	} else {
		log.Errorf("Unlock %s failed, err:%v.\n", lockObj, err)
		return LockDbErr
	}
}

func (ad *adapter) LockSched(tenantId, planId string) int {
	ss := ad.s.Copy()
	defer ss.Close()

	key := tenantId + "_" + planId
	return lock(ss, key, 30) //One schedule is supposed to be finished in 30 seconds
}

func (ad *adapter) UnlockSched(tenantId, planId string) int {
	ss := ad.s.Copy()
	defer ss.Close()

	key := tenantId + "_" + planId
	return unlock(ss, key)
}

func (ad *adapter) LockBucketLifecycleSched(bucketName string) int {
	ss := ad.s.Copy()
	defer ss.Close()

	return lock(ss, bucketName, 300) //One schedule is supposed to be finished in 300 seconds
}

func (ad *adapter) UnlockBucketLifecycleSched(bucketName string) int {
	ss := ad.s.Copy()
	defer ss.Close()

	return unlock(ss, bucketName)
}

func (ad *adapter) CreatePolicy(ctx context.Context, pol *Policy) (*Policy, error) {
	ss := ad.s.Copy()
	defer ss.Close()

	log.Infof("mongo.Createpolicy:%+v\n", pol)
	pol.Id = bson.NewObjectId()
	err := ss.DB(DataBaseName).C(CollPolicy).Insert(&pol)
	if err != nil {
		log.Errorf("Add policy to database failed, err:%v\n", err)
		return nil, ERR_DB_ERR
	}

	return pol, nil
}

func (ad *adapter) DeletePolicy(ctx context.Context, id string) error {
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
	m := bson.M{"_id": bson.ObjectIdHex(id)}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return ERR_INNER_ERR
	}
	err = c.Find(m).One(&po)
	if err == mgo.ErrNotFound {
		log.Info("Delete policy: the specified policy does not exist.")
		return ERR_POLICY_NOT_EXIST
	} else if err != nil {
		log.Info("Delete policy: DB error.")
		return ERR_DB_ERR
	}
	//Check if the policy is used by any plan, if it is used then it cannot be deleted
	cc := ss.DB(DataBaseName).C(CollPlan)
	count, erro := cc.Find(bson.M{"policy_ref:$ref": CollPolicy, "policy_ref:$id": po.Id, "policy_ref.$db": DataBaseName}).Count()
	if erro != nil {
		log.Errorf("Delete policy failed, get related plan failed, err:%v.\n", erro)
		return ERR_DB_ERR
	} else if count > 0 {
		log.Info("Delete policy failed, it is used by plan.")
		return ERR_IS_USED_BY_PLAN
	}

	//Delete it from database
	err = c.Remove(bson.M{"_id": po.Id})
	if err == mgo.ErrNotFound {
		log.Info("Delete policy: the specified policy does not exist.")
		return ERR_POLICY_NOT_EXIST
	} else if err != nil {
		log.Errorf("Delete policy from database failed,err:%v.\n", err)
		return ERR_DB_ERR
	}
	return nil
}

func (ad *adapter) ListPolicy(ctx context.Context) ([]Policy, error) {
	ss := ad.s.Copy()
	defer ss.Close()

	//var query mgo.Query;
	pols := []Policy{}
	m := bson.M{}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return nil, ERR_INNER_ERR
	}
	err = ss.DB(DataBaseName).C(CollPolicy).Find(m).All(&pols)
	if err == mgo.ErrNotFound || len(pols) == 0 {
		log.Info("no policy found.")
		return nil, nil
	} else if err != nil {
		log.Errorf("list policy from database failed, err:%v\n", err)
		return nil, ERR_DB_ERR
	}
	return pols, nil
}

func (ad *adapter) GetPolicy(ctx context.Context, id string) (*Policy, error) {
	ss := ad.s.Copy()
	defer ss.Close()

	log.Infof("GetPolicy: id=%s\n", id)
	pol := Policy{}
	m := bson.M{"_id": bson.ObjectIdHex(id)}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return nil, ERR_INNER_ERR
	}

	err = ss.DB(DataBaseName).C(CollPolicy).Find(m).One(&pol)
	if err == mgo.ErrNotFound {
		log.Error("plan does not exist.")
		return nil, ERR_POLICY_NOT_EXIST
	}

	return &pol, nil
}

func (ad *adapter) UpdatePolicy(ctx context.Context, newPol *Policy) (*Policy, error) {
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
	m := bson.M{"_id": newPol.Id}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return nil, ERR_INNER_ERR
	}
	err = ss.DB(DataBaseName).C(CollPolicy).Update(m, newPol)
	if err == mgo.ErrNotFound {
		//log.Info("Update policy failed, err: the specified policy does not exist.")
		log.Errorf("Update policy in database failed, err: %v.", err)
		return nil, ERR_POLICY_NOT_EXIST
	} else if err != nil {
		//log.Errorf("Update policy in database failed, err: %v.\n", err)
		log.Errorf("Update policy in database failed, err: %v.", err)
		return nil, ERR_DB_ERR
	}

	log.Info("Update policy succeefully.")
	return newPol, nil
}

func (ad *adapter) CreatePlan(ctx context.Context, plan *Plan) (*Plan, error) {
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

	//Check if name is duplicate
	pols := []Policy{}
	m := bson.M{"name": plan.Name}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return nil, ERR_INNER_ERR
	}
	err = c.Find(m).All(&pols)
	if err == nil && len(pols) != 0 {
		errmsg := fmt.Sprintf("duplicate name:%s", plan.Name)
		err = errors.New(errmsg)
		return nil, err
	}

	//Check if specific connector and policy exist or not
	err = checkPlanRelateObj(ctx, ss, plan)
	if err != nil {
		return nil, err
	}

	if plan.PolicyId != "" {
		if bson.IsObjectIdHex(plan.PolicyId) {
			plan.PolicyRef = mgo.DBRef{CollPolicy, bson.ObjectIdHex(plan.PolicyId), DataBaseName}
		} else {
			log.Errorf("invalid policy:%s\n", plan.PolicyId)
			return nil, ERR_POLICY_NOT_EXIST
		}
	}

	//Create plan id
	for i := 0; i < 3; i++ {
		plan.Id = bson.NewObjectId()
		err = c.Insert(plan)
		if err != nil && mgo.IsDup(err) {
			log.Errorf("Add plan into database failed, duplicate id:%s\n", string(plan.Id.Hex()))
			continue
		}
		break
	}

	return plan, err
}

func (ad *adapter) DeletePlan(ctx context.Context, id string) error {
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
	m := bson.M{"_id": bson.ObjectIdHex(id)}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return ERR_INNER_ERR
	}
	err = c.Find(m).One(&p)
	if err == mgo.ErrNotFound {
		log.Info("Delete plan failed, err:the specified p does not exist.")
		return ERR_PLAN_NOT_EXIST
	} else if err != nil {
		log.Errorf("Delete plan failed, err:%v.\n", err)
		return ERR_DB_ERR
	}

	//Delete it from database
	err = c.Remove(bson.M{"_id": p.Id})
	if err == mgo.ErrNotFound {
		log.Info("Delete plan failed, err:the specified p does not exist.")
		return ERR_PLAN_NOT_EXIST
	} else if err != nil {
		log.Errorf("Delete plan from database failed,err:%v.\n", err)
		return ERR_DB_ERR
	}

	log.Info("Delete plan successfully.")
	return nil
}

func checkPlanRelateObj(ctx context.Context, ss *mgo.Session, plan *Plan) error {
	if plan.PolicyId != "" {
		m := bson.M{"_id": bson.ObjectIdHex(plan.PolicyId)}
		err := UpdateContextFilter(ctx, m)
		if err != nil {
			return err
		}

		pol := Policy{}
		err = ss.DB(DataBaseName).C(CollPolicy).Find(m).One(&pol)
		if err != nil {
			log.Errorf("Err: the specific policy[id:%s] not exist.\n", plan.PolicyId)
			return ERR_POLICY_NOT_EXIST
		}
	}

	return nil
}

func (ad *adapter) UpdatePlan(ctx context.Context, plan *Plan) (*Plan, error) {
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
	err := checkPlanRelateObj(ctx, ss, plan)
	if err != nil {
		return nil, ERR_INNER_ERR
	}

	if plan.PolicyId != "" {
		if bson.IsObjectIdHex(plan.PolicyId) {
			plan.PolicyRef = mgo.DBRef{CollPolicy, bson.ObjectIdHex(plan.PolicyId), DataBaseName}
		} else {
			log.Errorf("invalid policy:%s\n", plan.PolicyId)
			return nil, ERR_POLICY_NOT_EXIST
		}
	} else {
		plan.PolicyRef = mgo.DBRef{}
	}

	//Update database
	m := bson.M{"_id": plan.Id}
	err = UpdateContextFilter(ctx, m)
	if err != nil {
		return nil, ERR_INNER_ERR
	}
	err = ss.DB(DataBaseName).C(CollPlan).Update(m, plan)
	if err == mgo.ErrNotFound {
		log.Errorf("update plan: the specified plan[id=%v] does not exist.", plan.Id)
		return nil, ERR_PLAN_NOT_EXIST
	} else if err != nil {
		log.Errorf("update plan in database failed, err: %v.\n", err)
		return nil, ERR_DB_ERR
	}
	return plan, nil
}

func (ad *adapter) ListPlan(ctx context.Context, limit int, offset int, filter interface{}) ([]Plan, error) {
	m := bson.M{}
	if filter != nil {
		if v, ok := filter.(map[string]string); ok {
			UpdateFilter(m, v)
		}
	}

	return ad.doListPlan(ctx, limit, offset, m)
}

func (ad *adapter) doListPlan(ctx context.Context, limit int, offset int, filter bson.M) ([]Plan,
	error) {
	//var query mgo.Query;
	ss := ad.s.Copy()
	defer ss.Close()

	log.Infof("Listplan filter:%v\n", filter)

	if v, ok := filter["bucketname"]; ok {
		query := []bson.M{}
		query = append(query, bson.M{"srcConn.bucketName": v})
		query = append(query, bson.M{"destConn.bucketName": v})
		filter["$or"] = query
		delete(filter, "bucketname")
	}

	err := UpdateContextFilter(ctx, filter)
	if err != nil {
		return nil, ERR_INNER_ERR
	}

	plans := []Plan{}
	err = ss.DB(DataBaseName).C(CollPlan).Find(filter).Skip(offset).Limit(limit).All(&plans)
	if err == mgo.ErrNotFound || len(plans) == 0 {
		log.Info("no plan found.")
		return nil, nil
	} else if err != nil {
		log.Errorf("get plan from database failed,err:%v.\n", err)
		return nil, ERR_DB_ERR
	}

	//Get the name of related policy and connectors
	for i := 0; i < len(plans); i++ {
		var pol Policy
		if plans[i].PolicyId != "" {
			log.Infof("PolicyRef:%+v\n", plans[i].PolicyRef)
			err := ss.DB(DataBaseName).FindRef(&plans[i].PolicyRef).One(&pol)
			if err != nil {
				log.Errorf("get PolicyRef failed,err:%v.\n", err)
				return nil, ERR_DB_ERR
			} else {
				plans[i].PolicyName = pol.Name
				//plans[i].PolicyId = string(pol.Id.Hex())
			}
		}
	}

	return plans, nil
}

func (ad *adapter) GetPlan(ctx context.Context, id string) (*Plan, error) {
	ss := ad.s.Copy()
	defer ss.Close()

	m := bson.M{"_id": bson.ObjectIdHex(id)}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return nil, ERR_INNER_ERR
	}

	p := Plan{}
	err = ss.DB(DataBaseName).C(CollPlan).Find(m).One(&p)
	if err == mgo.ErrNotFound {
		log.Error("plan does not exist.")
		return nil, ERR_PLAN_NOT_EXIST
	}

	//Get the name of related policy and connectors
	var pol Policy
	if p.PolicyId != "" {
		err := ss.DB(DataBaseName).FindRef(&p.PolicyRef).One(&pol)
		if err != nil {
			log.Errorf("get PolicyRef failed,err:%v.\n", err)
			return nil, ERR_DB_ERR
		} else {
			p.PolicyName = pol.Name
			//plans[i].PolicyId = string(pol.Id.Hex())
		}
	}
	return &p, nil
}

func (ad *adapter) GetPlanByPolicy(ctx context.Context, policyId string, limit int, offset int) ([]Plan, error) {
	log.Infof("GetPlanByPolicy: policyId=%s\n", policyId)
	m := bson.M{"policyId": policyId}

	return ad.doListPlan(ctx, limit, offset, m)
}

func (ad *adapter) CreateJob(ctx context.Context, job *Job) (*Job, error) {
	ss := ad.s.Copy()
	defer ss.Close()

	var err error
	i := 0
	for ; i < 3; i++ {
		job.Id = bson.NewObjectId()
		err = ss.DB(DataBaseName).C(CollJob).Insert(&job)
		if err != nil && mgo.IsDup(err) {
			log.Errorf("Add job into database failed, duplicate id:%s\n", string(job.Id.Hex()))
			continue
		}
		break
	}
	if i == 3 {
		log.Error("add job to database failed too much times.")
	}

	return job, err
}

func (ad *adapter) GetJob(ctx context.Context, id string) (*Job, error) {
	ss := ad.s.Copy()
	defer ss.Close()

	m := bson.M{"_id": bson.ObjectIdHex(id)}
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return nil, ERR_INNER_ERR
	}
	job := Job{}
	err = ss.DB(DataBaseName).C(CollJob).Find(m).One(&job)
	if err == mgo.ErrNotFound {
		log.Info("Job does not exist.")
		return nil, ERR_JOB_NOT_EXIST
	}
	return &job, nil
}

func (ad *adapter) ListJob(ctx context.Context, limit int, offset int, query interface{}) ([]Job, error) {
	ss := ad.s.Copy()
	defer ss.Close()

	m := bson.M{}
	UpdateFilter(m, query.(map[string]string))
	err := UpdateContextFilter(ctx, m)
	if err != nil {
		return nil, ERR_INNER_ERR
	}

	jobs := []Job{}
	err = ss.DB(DataBaseName).C(CollJob).Find(m).Skip(offset).Limit(limit).All(&jobs)
	if err == mgo.ErrNotFound || len(jobs) == 0 {
		log.Info("no jobs found.")
		return nil, nil
	} else if err != nil {
		log.Errorf("get jobs from database failed,err:%v.\n", err)
		return nil, ERR_DB_ERR
	}
	return jobs, nil
}
