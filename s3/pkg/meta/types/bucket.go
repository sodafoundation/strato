package types

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/opensds/multi-cloud/s3/pkg/helper"
	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

const (
	FIELD_NAME_BODY       = "body"
	FIELD_NAME_USAGE      = "usage"
	FIELD_NAME_FILECOUNTS = "file_counts"
)

type Bucket struct {
	*pb.Bucket
}

// implements the Serializable interface
func (b *Bucket) Serialize() (map[string]interface{}, error) {
	fields := make(map[string]interface{})
	bytes, err := helper.MsgPackMarshal(b)
	if err != nil {
		return nil, err
	}
	fields[FIELD_NAME_BODY] = string(bytes)
	fields[FIELD_NAME_USAGE] = b.Usages
	return fields, nil
}

func (b *Bucket) Deserialize(fields map[string]string) (interface{}, error) {
	body, ok := fields[FIELD_NAME_BODY]
	if !ok {
		return nil, errors.New(fmt.Sprintf("no field %s found", FIELD_NAME_BODY))
	}

	err := helper.MsgPackUnMarshal([]byte(body), b)
	if err != nil {
		return nil, err
	}
	if usageStr, ok := fields[FIELD_NAME_USAGE]; ok {
		b.Usages, err = strconv.ParseInt(usageStr, 10, 64)
		if err != nil {
			return nil, err
		}
	}

	return b, nil
}

func (b *Bucket) String() (s string) {
	s += "Name: " + b.Name + "\n"
	s += "CreateTime: " + time.Unix(b.CreateTime, 0).Format(CREATE_TIME_LAYOUT) + "\n"
	s += "TenantId: " + b.TenantId + "\n"
	s += "DefaultLocation: " + b.DefaultLocation + "\n"
	s += "CORS: " + fmt.Sprintf("%+v", b.Cors) + "\n"
	s += "ACL: " + fmt.Sprintf("%+v", b.Acl) + "\n"
	s += "LifeCycle: " + fmt.Sprintf("%+v", b.LifecycleConfiguration) + "\n"
	s += "Policy: " + fmt.Sprintf("%+v", b.BucketPolicy) + "\n"
	s += "Versioning: " + fmt.Sprintf("%+v", b.Versioning) + "\n"
	s += "Usage: " + humanize.Bytes(uint64(b.Usages)) + "\n"
	return
}

/* Learn from this, http://stackoverflow.com/questions/33587227/golang-method-sets-pointer-vs-value-receiver */
/* If you have a T and it is addressable you can call methods that have a receiver type of *T as well as methods that have a receiver type of T */
func (b *Bucket) GetValues() (values map[string]map[string][]byte, err error) {
	cors, err := json.Marshal(b.Cors)
	if err != nil {
		return
	}
	lc, err := json.Marshal(b.LifecycleConfiguration)
	if err != nil {
		return
	}

	var usage bytes.Buffer
	err = binary.Write(&usage, binary.BigEndian, b.Usages)
	if err != nil {
		return
	}
	values = map[string]map[string][]byte{
		BUCKET_COLUMN_FAMILY: map[string][]byte{
			"UID":        []byte(b.TenantId),
			"ACL":        []byte(b.Acl.CannedAcl),
			"CORS":       cors,
			"LC":         lc,
			"createTime": []byte(time.Unix(b.CreateTime, 0).Format(CREATE_TIME_LAYOUT)),
			"usage":      usage.Bytes(),
		},
		// TODO fancy ACL
	}
	return
}

func (b Bucket) GetCreateSql() (string, []interface{}) {
	acl, _ := json.Marshal(b.Acl)
	cors, _ := json.Marshal(b.Cors)
	lc, _ := json.Marshal(b.LifecycleConfiguration)
	bucket_policy, _ := json.Marshal(b.BucketPolicy)
	replia, _ := json.Marshal(b.ReplicationConfiguration)
	//createTime := time.Unix(b.CreateTime, 0).Format(TIME_LAYOUT_TIDB)
	createTime := time.Now().Format(TIME_LAYOUT_TIDB)
	log.Infof("createTime=%v\n", createTime)

	sql := "insert into buckets(bucketname,tenantid,userid,createtime,usages,location,acl,cors,lc,policy,versioning," +
		"replication) values(?,?,?,?,?,?,?,?,?,?,?,?);"
	args := []interface{}{b.Name, b.TenantId, b.UserId, createTime, b.Usages, b.DefaultLocation, acl, cors, lc,
		bucket_policy, b.Versioning.Status, replia}
	return sql, args
}
