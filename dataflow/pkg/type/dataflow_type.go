package _type

import (
	"github.com/globalsign/mgo/bson"
	"github.com/globalsign/mgo"
	"time"
)

type ErrCode int64

const (
	ERR_NOT_USED ErrCode = iota
	ERR_OK
	ERR_INNER_ERR
	ERR_DB_ERR
	ERR_INVALID_POLICY_NAME
	ERR_POLICY_NOT_EXIST
	ERR_INVALID_CONN_NAME
	ERR_CONN_NOT_EXIST
	ERR_PLAN_NOT_EXIST
	ERR_INVALID_PLAN_NAME
	ERR_NO_DEST_SRC_CONN_INVALID
	ERR_DEST_CONN_NOT_EXIST
	ERR_SRC_CONN_NOT_EXIST
	ERR_IS_USED //Connector or policy is used by plan
	ERR_RUN_PLAN_BUSY  //Plan is being scheduled
	ERR_JOB_NOT_EXIST
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

type Schedule struct {
	//Id bson.ObjectId		`json:"-" bson:"_id,omitempty"`
	Type string 			`json:"type" bson:"type"`
	Day []string			`json:"day" bson:"day"`
	TimePoint string		`json:"time_point" bson:"time_point"`
}

type Connector struct {
	Id bson.ObjectId		`json:"-" bson:"_id,omitempty"`
	Name string 			`json:"name" bson:"name"`
	SelfDef bool			`json:"self_def" bson:"self_def"`
	BucketName string 		`json:"bucket_name" bson:"bucket_name"`
	StorType   string		`json:"stor_type" bson:"store_type"`
	StorLocation string		`json:"stor_location" bson:"stor_location"` //s3 location or nas path
	//RemoteBucket string		`json:"remote_bucket" bson:"remote_bucket"`
	AccessKey	string		`json:"ak" bson:"ak"`
	SecreteKey	string 		`json:"sk" bson:"sk"`
	UserName 	string		`json:"user_name" bson:"user_name"`
	Passwd		string		`json:"passwd" bson:"passwd"`
	Tenant string 			`json:"tenant" bson:"tenant"`
}

type Filter struct {
	Prefix string			`json:"prefix" bson:"prefix"`
	BucketName string		`json:"bucket_name" bson:"bucket_name"`
}

type Plan struct {
	Id bson.ObjectId		`json:"-" bson:"_id,omitempty"`
	Name string 			`json:"name" bson:"name"`
	Description string 		`json:"description" bson:"description"`
	//IsSched	bool			`json:"is_sched" bson:"is_sched"`
	//SchedServer string		`json:"sched_server" bson:"sched_server"`
	Type string				`json:"type" bson:"type"` //migration
	SourceConnId  string	`json:"src_conn_id" bson:"src_conn_id"`
	SourceConnRef	mgo.DBRef	`json:"src_conn_ref" bson:"src_conn_ref"`
	SourceConnName  string	`json:"src_conn_name" bson:"src_conn_name"`
	DestConnId    string	`json:"dest_conn_id" bson:"dest_conn_id"`
	DestConnRef 	mgo.DBRef	`json:"dest_conn_ref" bson:"dest_conn_ref"`
	DestConnName    string	`json:"dest_conn_name" bson:"dest_conn_name"`
	SourceDir	string		`json:"source_dir" bson:"source_dir"`
	DestDir		string		`json:"dest_dir" bson:"dest_dir"`
	OverWrite	bool		`json:"over_write" bson:"over_write"`
	RemainSource	bool	`json:"remain_source" bson:"remain_source"`
	LastSchedTime	int64	`json:"last_sched_time" bson:"last_sched_time"`
	PolicyId		string	`json:"policy_id" bson:"policy_id"`
	PolicyRef	 mgo.DBRef	`json:"policy_ref" bson:"policy_ref"`
	PolicyName		string  `json:"policy_name" bson:"policy_name"`
	Tenant string 			`json:"tenant" bson:"tenant"`
}

type Job struct{
	Id bson.ObjectId 		`json:"-" bson:"_id"`  //make index on id, it cnanot be duplicate
	Type string 			`json:"type" bson:"type"` //migration
	PlanName string			`json:"plan_name" bson:"plan_name"`
	PlanId string			`json:"plan_id" bson:"plan_id"`
	TotalCount int 			`json:"total_count" bson:"total_count"`
	PassedCount int			`json:"passed_count" bson:"passed_count"`
	TotalCapacity int64		`json:"total_capacity" bson:"passed_capacity"`
	SourceLocation string	`json:"source_location" bson:"source_location"`
	SourceBucket string		`json:"src_bucket" bson:"dest_bucket"`  //Bucket in OpenSDS
	DestLocation string		`json:"source_location" bson:"source_location"`
	DestBucket string		`json:"dest_bucket" bson:"dest_bucket"` //Bucket in OpenSDS
	//SourceBackend string	`json:"source_backend" bson:"source_backend"`
	//DestBackend string	`json:"dest_backend" bson:"dest_backend"`
	CreateTime time.Time 	`json:"create_time" bson:"create_time"`
	EndTime time.Time		`json:"end_time" bson:"end_time"`
}

type Backend struct{
	Id bson.ObjectId 		`json:"-" bson:"_id,omitempty"`
	Type string				`json:"type" bson:"type"` //aws-obj,azure-obj,hw-obj
	Location string			`json:"location" bson:"location"`
	AccessKey string			`json:"ak" bson:"ak"`
	SecreteKey string		`json:"sk" bson:"sk"`
}