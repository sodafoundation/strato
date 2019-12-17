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

package model

import (
	"errors"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

var (
	STOR_TYPE_OPENSDS          = "opensds-obj"
	STOR_TYPE_AWS_S3           = "aws-s3"
	STOR_TYPE_AZURE_BLOB       = "azure-blob"
	STOR_TYPE_HW_OBS           = "hw-obs"
	STOR_TYPE_CEPH_S3          = "ceph-s3"
	STOR_TYPE_GCP_S3           = "gcp-s3"
	STOR_TYPE_HW_FUSIONSTORAGE = "fusionstorage-object"
	STOR_TYPE_HW_FUSIONCLOUD   = "hw-fusioncloud"
	STOR_TYPE_IBM_COS          = "ibm-cos"
)

var (
	JOB_STATUS_PENDING   = "pending"
	JOB_STATUS_RUNNING   = "running"
	JOB_STATUS_SUCCEED   = "succeed"
	JOB_STATUS_FAILED    = "failed"
	JOB_STATUS_ABORTED   = "aborted"
	JOB_STATUS_CANCELLED = "cancelled"
)

var (
	//ERR_NOT_USED ErrCode = iota
	//ERR_OK = errors.New("succeed")
	ERR_INNER_ERR           = errors.New("inner error")
	ERR_DB_ERR              = errors.New("database error")
	ERR_INVALID_POLICY_NAME = errors.New("invalid policy name")
	ERR_POLICY_NOT_EXIST    = errors.New("policy does not exist")
	ERR_INVALID_CONN_NAME   = errors.New("invalid connector name")
	ERR_CONN_NOT_EXIST      = errors.New("connector does not esit")
	ERR_PLAN_NOT_EXIST      = errors.New("plan does not exist")
	ERR_INVALID_PLAN_NAME   = errors.New("invalid plan name")
	ERR_DEST_SRC_CONN_EQUAL = errors.New("source is the same as destination")
	ERR_DEST_CONN_NOT_EXIST = errors.New("invalid destination connector")
	ERR_SRC_CONN_NOT_EXIST  = errors.New("invalid source connector")
	ERR_IS_USED_BY_PLAN     = errors.New("is used by plan") //Connector or policy is used by plan
	ERR_RUN_PLAN_BUSY       = errors.New("is scheduling")   //Plan is being scheduled
	ERR_JOB_NOT_EXIST       = errors.New("job not exist")
	ERR_PLAN_NOT_IN_TRIGGER = errors.New("specified plan is not in trigger")
)

var (
	DefaultLimit  = 1000
	DefaultOffset = 0
)

const (
	LockSuccess = 0
	LockBusy    = 1
	LockDbErr   = 2
)

type Policy struct {
	Id          bson.ObjectId `json:"-" bson:"_id,omitempty"`
	TenantId    string        `json:"tenantId" bson:"tenantId"`
	UserId      string        `json:"userId" bson:"userId"`
	Name        string        `json:"name" bson:"name"`
	Description string        `json:"description" bson:"description"`
	Schedule    Schedule      `json:"schedule" bson:"schedule"`
}

const (
	ScheduleTypeCron = "cron"
)

type Schedule struct {
	Type              string `json:"type" bson:"type"`
	TriggerProperties string `json:"triggerProperties" bson:"triggerProperties"`
}

type KeyValue struct {
	Key   string `json:"key" bson:"key"`
	Value string `json:"value" bson:"value"`
}

type Connector struct {
	StorType   string     `json:"storType" bson:"storeType"`    //opensds-obj, aws-obj,azure-obj,ceph-s3,hw-obj,nas
	BucketName string     `json:"bucketName" bson:"bucketName"` //when StorType is opensds, need this
	ConnConfig []KeyValue `json:"connConfig" bson:"connConfig"`
}

type Filter struct {
	Prefix string     `json:"prefix" bson:"prefix"`
	Tag    []KeyValue `json:"tag" bson:"tag"`
}

type Plan struct {
	Id          bson.ObjectId `json:"-" bson:"_id,omitempty"`
	Name        string        `json:"name" bson:"name"`
	Description string        `json:"description" bson:"description"`
	//IsSched	bool			`json:"is_sched" bson:"is_sched"`
	//SchedServer string		`json:"sched_server" bson:"sched_server"`
	Type          string    `json:"type" bson:"type"` //migration
	SourceConn    Connector `json:"srcConn" bson:"srcConn"`
	DestConn      Connector `json:"destConn" bson:"destConn"`
	Filter        Filter    `json:"filter" bson:"filter"`
	RemainSource  bool      `json:"remainSource" bson:"remainSource"`
	LastSchedTime int64     `json:"lastSchedTime" bson:"lastSchedTime"`
	PolicyId      string    `json:"policyId" bson:"policyId"`
	PolicyRef     mgo.DBRef `json:"policyRef" bson:"policyRef"`
	PolicyName    string    `json:"policyName" bson:"policyName"`
	PolicyEnabled bool      `json:"policyEnabled" bson:"policyEnabled"`
	TenantId      string    `json:"tenantId" bson:"tenantId"`
	UserId        string    `json:"userId" bson:"userId"`
}

const (
	TriggerTypeManual = "manual"
	TriggerTypeAuto   = "auto"
)

type Job struct {
	Id               bson.ObjectId `json:"-" bson:"_id"` //make index on id, it cnanot be duplicate
	TriggerType      string        `json:"triggerType" bson:"triggerType"`
	Type             string        `json:"type" bson:"type"` //migration
	PlanName         string        `json:"planName" bson:"planName"`
	PlanId           string        `json:"planId" bson:"planId"`
	TotalCount       int64         `json:"totalCount" bson:"totalCount"`
	PassedCount      int64         `json:"passedCount" bson:"passedCount"`
	TotalCapacity    int64         `json:"totalCapacity" bson:"totalCapacity"`
	PassedCapacity   int64         `json:"passedCapacity" bson:"passedCapacity"`
	MigratedCapacity float64       `json:"migratedCapacity" bson:"migratedCapacity"`
	//when the plan related connector type is OPENSDS, then location should be bucket name
	SourceLocation string    `json:"sourceLocation" bson:"sourceLocation"`
	DestLocation   string    `json:"destLocation" bson:"destLocation"`
	CreateTime     time.Time `json:"createTime" bson:"createTime"`
	StartTime      time.Time `json:"startTime" bson:"startTime"`
	EndTime        time.Time `json:"endTime" bson:"endTime"`
	RemainSource   bool      `json:"remainSource" bson:"remainSource"`
	Status         string    `json:"status" bson:"status"` //queueing,
	TenantId       string    `json:"tenantId" bson:"tenantId"`
	UserId         string    `json:"userId" bson:"userId"`
	Progress       int64     `json:"progress" bson:"progress"`
}

type Backend struct {
	Id         bson.ObjectId `json:"-" bson:"_id,omitempty"`
	Type       string        `json:"type" bson:"type"` //aws-obj,azure-obj,hw-obj
	Location   string        `json:"location" bson:"location"`
	AccessKey  string        `json:"ak" bson:"ak"`
	SecreteKey string        `json:"sk" bson:"sk"`
}
