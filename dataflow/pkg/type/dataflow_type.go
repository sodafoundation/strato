package _type

import (
	"github.com/globalsign/mgo/bson"
	"github.com/globalsign/mgo"
	"time"
	"errors"
)

var (
	STOR_TYPE_OPENSDS = "opensds-obj"
	STOR_TYPE_AWS_S3 = "aws-obj"
	STOR_TYPE_AZURE_BLOB = "azure-obj"
	STOR_TYPE_HW_OBS = "hw-obj"
	STOR_TYPE_HW_FUSIONSTORAGE = "hw-fusionstorage"
	STOR_TYPE_HW_FUSIONCLOUD = "hw-fusioncloud"
)

var (
	JOB_STATUS_PENDING = "pending"
	JOB_STATUS_RUNNING = "rnning"
	JOB_STATUS_SUCCEED = "succeed"
	JOB_STATUS_FAILED =  "failed"
)

var (
	//ERR_NOT_USED ErrCode = iota
	//ERR_OK = errors.New("succeed")
	ERR_INNER_ERR = errors.New("inner error")
	ERR_DB_ERR = errors.New("database error")
	ERR_INVALID_POLICY_NAME = errors.New("invalid policy name")
	ERR_POLICY_NOT_EXIST = errors.New("policy does not exist")
	ERR_INVALID_CONN_NAME = errors.New("invalid connector name")
	ERR_CONN_NOT_EXIST = errors.New("connector does not esit")
	ERR_PLAN_NOT_EXIST = errors.New("plan does not exist")
	ERR_INVALID_PLAN_NAME = errors.New("invalid plan name")
	ERR_DEST_SRC_CONN_EQUAL = errors.New("source is the same as destination")
	ERR_DEST_CONN_NOT_EXIST = errors.New("invalid destination connector")
	ERR_SRC_CONN_NOT_EXIST = errors.New("invalid source connector")
	ERR_IS_USED_BY_PLAN = errors.New("is used by plan")//Connector or policy is used by plan
	ERR_RUN_PLAN_BUSY  = errors.New("is scheduling")//Plan is being scheduled
	ERR_JOB_NOT_EXIST = errors.New("job not exist")
)

const (
	LockSuccess = 0
	LockBusy  = 1
	LockDbErr	   = 2
)

type Policy struct {
	Id bson.ObjectId		`json:"-" bson:"_id,omitempty"`
	Name string				`json:"name" bson:"name"`
	Description string 		`json:"description" bson:"description"`
	Schedule Schedule		`json:"schedule" bson:"schedule"`
	Tenant string 			`json:"tenant" bson:"tenant"`
}

const (
	ScheduleTypeCron  = "cron"
)

type Schedule struct {
	//Id bson.ObjectId		`json:"-" bson:"_id,omitempty"`
	Type string 			`json:"type" bson:"type"`
	Day []string			`json:"day,omitempty" bson:"day,omitempty"`
	TimePoint string		`json:"time_point,omitempty" bson:"time_point,omitempty"`
	TriggerProperties string `json:"trigger_properties" bson:"trigger_properties"`
}

type KeyValue struct {
	Key string
	Value string
}

type Connector struct {
	StorType   string		`json:"stor_type" bson:"store_type"` //opensds-obj, aws-obj,azure-obj,ceph-obj,hw-obj,nas
	BucketName string 		`json:"bucket_name" bson:"bucket_name"`//when StorType is opensds, need this
	ConnConfig []KeyValue   `json:"conn_config" bson:"conn_config"`
}

type Filter struct {
	Prefix string			`json:"prefix" bson:"prefix"`
	Tag []KeyValue		    `json:"tag" bson:"tag"`
}

type Plan struct {
	Id bson.ObjectId		`json:"-" bson:"_id,omitempty"`
	Name string 			`json:"name" bson:"name"`
	Description string 		`json:"description" bson:"description"`
	//IsSched	bool			`json:"is_sched" bson:"is_sched"`
	//SchedServer string		`json:"sched_server" bson:"sched_server"`
	Type string				`json:"type" bson:"type"` //migration
	SourceConn  Connector	`json:"src_conn" bson:"src_conn"`
	DestConn  Connector	    `json:"dest_conn" bson:"dest_conn"`
	Filt	Filter		    `json:"source_dir" bson:"source_dir"`
	OverWrite	bool		`json:"over_write" bson:"over_write"`
	RemainSource	bool	`json:"remain_source" bson:"remain_source"`
	LastSchedTime	int64	`json:"last_sched_time" bson:"last_sched_time"`
	PolicyId		string	`json:"policy_id" bson:"policy_id"`
	PolicyRef	 mgo.DBRef	`json:"policy_ref" bson:"policy_ref"`
	PolicyName		string  `json:"policy_name" bson:"policy_name"`
	Tenant string 			`json:"tenant" bson:"tenant"`
}

const (
	TriggerTypeManual = "manual"
	TriggerTypeAuto = "auto"
)

type Job struct{
	Id bson.ObjectId 		`json:"-" bson:"_id"`  //make index on id, it cnanot be duplicate
	TriggerType string 		`json:"trigger_type" bson:"trigger_type"`
	Type string 			`json:"type" bson:"type"` //migration
	PlanName string			`json:"plan_name" bson:"plan_name"`
	PlanId string			`json:"plan_id" bson:"plan_id"`
	TotalCount int 			`json:"total_count" bson:"total_count"`
	PassedCount int			`json:"passed_count" bson:"passed_count"`
	TotalCapacity int64		`json:"total_capacity" bson:"total_capacity"`
	PassedCapacity int64    `json:"passed_capacity" bson:"passed_capacity"`
	//when the plan related connector type is OPENSDS, then location should be bucket name
	SourceLocation string	`json:"source_location" bson:"source_location"`
	DestLocation string		`json:"dest_location" bson:"dest_location"`
	CreateTime time.Time 	`json:"create_time" bson:"create_time"`
	StartTime time.Time 	`json:"start_time" bson:"start_time"`
	EndTime time.Time		`json:"end_time" bson:"end_time"`
	OverWrite	bool		`json:"over_write" bson:"over_write"`
	RemainSource	bool	`json:"remain_source" bson:"remain_source"`
	Status 	string			`json:"status" bson:"status"` //queueing,
	Tenant string 			`json:"tenant" bson:"tenant"`
}

type Backend struct{
	Id bson.ObjectId 		`json:"-" bson:"_id,omitempty"`
	Type string				`json:"type" bson:"type"` //aws-obj,azure-obj,hw-obj
	Location string			`json:"location" bson:"location"`
	AccessKey string			`json:"ak" bson:"ak"`
	SecreteKey string		`json:"sk" bson:"sk"`
}
