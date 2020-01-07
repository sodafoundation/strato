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
package tidbclient

import (
	"context"
	"database/sql"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/opensds/multi-cloud/api/pkg/common"
	. "github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/pkg/helper"
	. "github.com/opensds/multi-cloud/s3/pkg/meta/types"
	"github.com/opensds/multi-cloud/s3/pkg/meta/util"
	"github.com/opensds/multi-cloud/s3/pkg/utils"
	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

const TimeDur = 5000 // milisecond

func (t *TidbClient) GetBucket(ctx context.Context, bucketName string) (bucket *Bucket, err error) {
	log.Infof("get bucket[%s] from tidb ...\n", bucketName)
	var acl, cors, lc, policy, replication, createTime string
	var updateTime sql.NullString
	var row *sql.Row

	sqltext := "select bucketname,tenantid,createtime,usages,location,acl,cors,lc,policy,versioning,replication," +
		"update_time from buckets where bucketname=?;"
	row = t.Client.QueryRow(sqltext, bucketName)

	tmp := &Bucket{Bucket: &pb.Bucket{}}
	tmp.Versioning = &pb.BucketVersioning{}
	err = row.Scan(
		&tmp.Name,
		&tmp.TenantId,
		&createTime,
		&tmp.Usages,
		&tmp.DefaultLocation,
		&acl,
		&cors,
		&lc,
		&policy,
		&tmp.Versioning.Status,
		&replication,
		&updateTime,
	)
	if err != nil {
		err = handleDBError(err)
		return
	}

	ct, err := time.Parse(TIME_LAYOUT_TIDB, createTime)
	if err != nil {
		err = handleDBError(err)
		return
	}
	tmp.CreateTime = ct.Unix()

	pbAcl := pb.Acl{}
	err = json.Unmarshal([]byte(acl), &pbAcl)
	if err != nil {
		err = handleDBError(err)
		return
	}
	tmp.Acl = &pbAcl

	err = json.Unmarshal([]byte(cors), &tmp.Cors)
	if err != nil {
		err = handleDBError(err)
		return
	}
	err = json.Unmarshal([]byte(lc), &tmp.LifecycleConfiguration)
	if err != nil {
		err = handleDBError(err)
		return
	}
	err = json.Unmarshal([]byte(policy), &tmp.BucketPolicy)
	if err != nil {
		err = handleDBError(err)
		return
	}
	err = json.Unmarshal([]byte(replication), &tmp.ReplicationConfiguration)
	if err != nil {
		err = handleDBError(err)
		return
	}
	//get versioning for the bucket
	versionOpts, versionErr := t.GetBucketVersioning(ctx, tmp.Name)
	if versionErr != nil {
		log.Error("error in getting versioning information, err:%v\n", versionErr)
		err = handleDBError(versionErr)
		return
	}
	tmp.Versioning = &pb.BucketVersioning{}
	if versionOpts != nil {
		tmp.Versioning.Status = versionOpts.Status
	}

	// get SSE info for this bucket
	tmp.ServerSideEncryption = &pb.ServerSideEncryption{}
	sseOpts, sseErr := t.GetBucketSSE(ctx, tmp.Name)
	if sseErr != nil {
		return
	}
	sseType := "NONE"
	if sseOpts != nil {
		sseType = sseOpts.SseType
		tmp.ServerSideEncryption.EncryptionKey = sseOpts.EncryptionKey
	}
	tmp.ServerSideEncryption.SseType = sseType
	bucket = tmp
	return
}

// For the request that list buckets, better to filter according to tenant.
func (t *TidbClient) GetBuckets(ctx context.Context) (buckets []*Bucket, err error) {
	log.Info("list buckets from tidb ...")

	isAdmin, tenantId, _, err := util.GetCredentialFromCtx(ctx)
	if err != nil {
		return nil, ErrInternalError
	}

	var rows *sql.Rows
	sqltext := "select bucketname,tenantid,userid,createtime,usages,location,deleted,acl,cors,lc,policy," +
		"versioning,replication,update_time from buckets;"

	if !isAdmin {
		sqltext = "select bucketname,tenantid,userid,createtime,usages,location,deleted,acl,cors,lc,policy," +
			"versioning,replication,update_time from buckets where tenantid=?;"
		rows, err = t.Client.Query(sqltext, tenantId)
	} else {
		rows, err = t.Client.Query(sqltext)
	}

	if err == sql.ErrNoRows {
		err = nil
		return
	} else if err != nil {
		err = handleDBError(err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		tmp := Bucket{Bucket: &pb.Bucket{}}
		bucketVer := pb.BucketVersioning{}
		tmp.Versioning = &bucketVer
		var acl, cors, lc, policy, createTime, replication string
		var updateTime sql.NullString
		err = rows.Scan(
			&tmp.Name,
			&tmp.TenantId,
			&tmp.UserId,
			&createTime,
			&tmp.Usages,
			&tmp.DefaultLocation,
			&tmp.Deleted,
			&acl,
			&cors,
			&lc,
			&policy,
			&tmp.Versioning.Status,
			&replication,
			&updateTime)
		if err != nil {
			err = handleDBError(err)
			return
		}

		//get versioning for the bucket
		versionOpts, versionErr := t.GetBucketVersioning(ctx, tmp.Name)
		if versionErr != nil {
			log.Error("error in getting versioning information, err:%v\n", versionErr)
			err = handleDBError(versionErr)
			return
		}
		tmp.Versioning = &pb.BucketVersioning{}
		if versionOpts != nil {
			tmp.Versioning.Status = versionOpts.Status
		}

		// get SSE info for this bucket
		sseOpts, sseErr := t.GetBucketSSE(ctx, tmp.Name)
		if sseErr != nil {
			return
		}
		sseType := "NONE"
		if sseOpts != nil {
			sseType = sseOpts.SseType
		}
		tmp.ServerSideEncryption = &pb.ServerSideEncryption{}
		tmp.ServerSideEncryption.SseType = sseType

		var ctime time.Time
		ctime, err = time.ParseInLocation(TIME_LAYOUT_TIDB, createTime, time.Local)
		if err != nil {
			err = handleDBError(err)
			return
		}
		tmp.CreateTime = ctime.Unix()
		err = json.Unmarshal([]byte(acl), &tmp.Acl)
		if err != nil {
			err = handleDBError(err)
			return
		}
		err = json.Unmarshal([]byte(cors), &tmp.Cors)
		if err != nil {
			err = handleDBError(err)
			return
		}
		err = json.Unmarshal([]byte(lc), &tmp.LifecycleConfiguration)
		if err != nil {
			err = handleDBError(err)
			return
		}
		err = json.Unmarshal([]byte(policy), &tmp.BucketPolicy)
		if err != nil {
			err = handleDBError(err)
			return
		}
		err = json.Unmarshal([]byte(replication), &tmp.ReplicationConfiguration)
		if err != nil {
			err = handleDBError(err)
			return
		}
		buckets = append(buckets, &tmp)
	}
	return
}

//Actually this method is used to update bucket
func (t *TidbClient) PutBucket(ctx context.Context, bucket *Bucket) error {
	log.Infof("put bucket[%s] into tidb ...\n", bucket.Name)
	acl, _ := json.Marshal(bucket.Acl)
	cors, _ := json.Marshal(bucket.Cors)
	lc, _ := json.Marshal(bucket.LifecycleConfiguration)
	bucket_policy, _ := json.Marshal(bucket.BucketPolicy)
	sql := "update buckets set bucketname=?,acl=?,policy=?,cors=?,lc=?,tenantid=?,versioning=? where bucketname=?"
	args := []interface{}{bucket.Name, acl, bucket_policy, cors, lc, bucket.TenantId, bucket.Versioning.Status, bucket.Name}

	_, err := t.Client.Exec(sql, args...)
	if err != nil {
		return handleDBError(err)
	}

	return nil
}

func (t *TidbClient) CheckAndPutBucket(ctx context.Context, bucket *Bucket) (bool, error) {
	var processed bool
	_, err := t.GetBucket(ctx, bucket.Name)
	if err == nil {
		processed = false
		return processed, err
	} else if err != ErrNoSuchKey {
		processed = false
		return processed, err
	} else {
		processed = true
	}
	log.Infof("insert bucket[%s] into database.\n", bucket.Name)
	sql, args := bucket.GetCreateSql()
	_, err = t.Client.Exec(sql, args...)
	if err != nil {
		err = handleDBError(err)

	}
	return processed, err
}

func (t *TidbClient) ListObjects(ctx context.Context, bucketName string, versioned bool, maxKeys int,
	filter map[string]string) (retObjects []*Object, appendInfo utils.ListObjsAppendInfo, err error) {
	const MaxObjectList = 10000
	// TODO: support versioning
	if versioned {
		log.Errorf("not supported.")
		err = ErrInternalError
		return
	}
	defer func() {
		err = handleDBError(err)
	}()
	var count int
	var exit bool
	objectMap := make(map[string]struct{})
	objectNum := make(map[string]int)
	commonPrefixes := make(map[string]struct{})
	if filter == nil {
		filter = make(map[string]string)
	}
	omarker := filter[common.KMarker]
	delimiter := filter[common.KDelimiter]
	prefix := filter[common.KPrefix]
	for {
		var loopcount int
		var sqltext string
		var rows *sql.Rows
		args := make([]interface{}, 0)
		// only select index column here to avoid slow query
		sqltext = "select bucketname,name,version from objects where bucketName=? and deletemarker=0"
		args = append(args, bucketName)
		var inargs []interface{}
		sqltext, inargs, err = buildSql(ctx, filter, sqltext)
		for _, v := range inargs {
			args = append(args, v)
		}
		log.Infof("sqltext:%s, args:%v\n", sqltext, args)
		tstart := time.Now()
		rows, err = t.Client.Query(sqltext, args...)
		if err != nil {
			return
		}
		tqueryend := time.Now()
		tdur := tqueryend.Sub(tstart).Nanoseconds()
		if tdur/1000000 > TimeDur {
			log.Debugf("slow query when list objects, sqltext:%s, args:%v, nanoseconds:%d\n", sqltext, args, tdur)
		}
		defer rows.Close()
		for rows.Next() {
			loopcount += 1
			//var name, lastModified string
			var bname, name string
			var version uint64
			err = rows.Scan(
				&bname,
				&name,
				&version,
			)
			if err != nil {
				log.Errorf("err:%v\n", err)
				return
			}
			//prepare next marker, marker is last bucketname got from the last query,
			//TODU: be sure how tidb/mysql compare strings
			if _, ok := objectNum[name]; !ok {
				objectNum[name] = 0
			}
			objectNum[name] += 1
			filter[common.KMarker] = name

			if _, ok := objectMap[name]; !ok {
				objectMap[name] = struct{}{}
			} else {
				continue
			}

			// TODO: filter by deletemarker if versioning enabled

			if name == omarker {
				log.Infof("%s euqals to omarker, continue\n", name)
				continue
			}

			// Group by delimiter, delimiter is used to group object keys. Object keys that contain the same string
			// between the prefix and the first occurrence of the delimiter to be rolled up into a single result element
			// in the CommonPrefixes collection. These rolled-up keys are not returned elsewhere in the response. Each
			// rolled-up result counts as only one return against the MaxKeys value. Those are compatible with AWS S3,
			// please see https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_ListObjectsV2.html for details.
			if len(delimiter) != 0 {
				subStr := strings.TrimPrefix(name, prefix)
				n := strings.Index(subStr, delimiter)
				if n != -1 {
					prefixKey := prefix + string([]byte(subStr)[0:(n+1)])
					filter[common.KMarker] = prefixKey[0:(len(prefixKey)-1)] + string(delimiter[len(delimiter)-1]+1)
					if prefixKey == omarker {
						continue
					}
					if _, ok := commonPrefixes[prefixKey]; !ok {
						if count == maxKeys {
							appendInfo.Truncated = true
							exit = true
							break
						}
						commonPrefixes[prefixKey] = struct{}{}
						// When response is truncated (the IsTruncated element value in the response is true), you can
						// use the key name in this field as marker in the subsequent request to get next set of objects,
						// the same as AWS S3. See https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_ListObjects.html
						// for details.
						appendInfo.NextMarker = prefixKey
						count += 1
					}
					continue
				}
			}

			var o *Object
			strVer := strconv.FormatUint(version, 10)
			o, err = t.GetObject(ctx, bname, name, strVer)
			if err != nil {
				log.Errorf("err:%v\n", err)
				return
			}

			count += 1
			if count == maxKeys {
				appendInfo.NextMarker = name
			}

			if count > maxKeys {
				appendInfo.Truncated = true
				exit = true
				break
			}

			retObjects = append(retObjects, o)
		}

		tend := time.Now()
		tdur = tend.Sub(tqueryend).Nanoseconds()
		if tdur/1000000 > TimeDur {
			log.Debugf("slow get when list objects, time:%d\n", tdur)
		}

		if loopcount < MaxObjectList {
			exit = true
		}
		if exit {
			break
		}

		err = rows.Err()
		if err != nil {
			log.Errorf("err:%v\n", err)
			return
		}
	}

	appendInfo.Prefixes = helper.Keys(commonPrefixes)

	return
}

func (t *TidbClient) CountObjects(ctx context.Context, bucketName, prefix string) (*utils.ObjsCountInfo, error) {
	var sqltext string
	rsp := utils.ObjsCountInfo{}
	var err error
	var sizeStr sql.NullString
	if prefix == "" {
		sqltext = "select count(*),sum(size) from objects where bucketname=? and tier<?;"
		err = t.Client.QueryRow(sqltext, bucketName, utils.Tier999).Scan(&rsp.Count, &sizeStr)
	} else {
		filt := prefix + "%"
		sqltext = "select count(*),sum(size) from objects where bucketname=? and tier<? and name like ?;"
		err = t.Client.QueryRow(sqltext, bucketName, utils.Tier999, filt).Scan(&rsp.Count, &sizeStr)
	}

	if err != nil {
		log.Errorf("db error:%v\n", err)
		return nil, err
	}

	if sizeStr.Valid {
		size, err := strconv.ParseInt(sizeStr.String, 10, 64)
		if err != nil {
			log.Errorf("error:%v\n", err)
		} else {
			rsp.Size = size
		}
	}

	return &rsp, err
}

func (t *TidbClient) DeleteBucket(ctx context.Context, bucket *Bucket) (err error) {
	var sqlTx *sql.Tx
	var tx interface{}
	tx, err = t.Client.Begin()

	defer func() {
		if err == nil {
			err = sqlTx.Commit()
		}
		if err != nil {
			sqlTx.Rollback()
		}
	}()

	sqlTx, _ = tx.(*sql.Tx)

	sqltext := "delete from buckets where bucketname=?;"
	_, err = sqlTx.Exec(sqltext, bucket.Name)
	if err != nil {
		err = handleDBError(err)
	}

	sqltextDelSSE := "delete from bucket_sseopts where bucketname=?;"

	_, err = sqlTx.Exec(sqltextDelSSE, bucket.Name)

	if err != nil {
		err = handleDBError(err)
	}

	return
}

func (t *TidbClient) UpdateUsage(ctx context.Context, bucketName string, size int64, tx interface{}) (err error) {
	var sqlTx *sql.Tx
	if tx == nil {
		tx, err = t.Client.Begin()

		defer func() {
			if err == nil {
				err = sqlTx.Commit()
			}
			if err != nil {
				sqlTx.Rollback()
			}
		}()
	}
	sqlTx, _ = tx.(*sql.Tx)

	sql := "update buckets set usages=? where bucketname=?;"
	_, err = sqlTx.Exec(sql, size, bucketName)
	if err != nil {
		err = handleDBError(err)
	}
	return
}

func (t *TidbClient) UpdateUsages(ctx context.Context, usages map[string]int64, tx interface{}) error {
	var sqlTx *sql.Tx
	var err error
	if nil == tx {
		tx, err = t.Client.Begin()
		defer func() {
			if nil == err {
				err = sqlTx.Commit()
			} else {
				sqlTx.Rollback()
			}
		}()
	}
	sqlTx, _ = tx.(*sql.Tx)
	sqlStr := "update buckets set usages = ? where bucketname = ?;"
	st, err := sqlTx.Prepare(sqlStr)
	if err != nil {
		log.Error("failed to prepare statment with sql: ", sqlStr, ", err: ", err)
		return ErrDBError
	}
	defer st.Close()

	for bucket, usage := range usages {
		_, err = st.Exec(usage, bucket)
		if err != nil {
			log.Error("failed to update usage for bucket: ", bucket, " with usage: ", usage, ", err: ", err)
			return ErrDBError
		}
	}
	return nil
}

func (t *TidbClient) ListBucketLifecycle(ctx context.Context) (buckets []*Bucket, err error) {
	log.Infoln("list bucket lifecycle from tidb ...\n")
	defer func() {
		err = handleDBError(err)
	}()

	var rows *sql.Rows
	sqltext := "select bucketname,lc from buckets where lc!=CAST('null' AS JSON);"
	rows, err = t.Client.Query(sqltext)
	if err == sql.ErrNoRows {
		err = nil
		return
	} else if err != nil {
		log.Errorf("db err:%v\n", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		tmp := Bucket{Bucket: &pb.Bucket{}}
		var lc string
		err = rows.Scan(
			&tmp.Name,
			&lc)
		if err != nil {
			log.Errorf("db err:%v\n", err)
			return
		}

		err = json.Unmarshal([]byte(lc), &tmp.LifecycleConfiguration)
		if err != nil {
			log.Errorf("db err:%v\n", err)
			return
		}

		buckets = append(buckets, &tmp)
	}

	err = rows.Err()
	if err != nil {
		log.Errorf("db err:%v\n", err)
	}

	return
}

func (t *TidbClient) UpdateBucketVersioning(ctx context.Context, bucketName string, versionStatus string) error {
	log.Infof("put bucket[%s] Version info[%s] into tidb ...\n", bucketName, versionStatus)

	sql := "update bucket_versionopts set versionstatus=? where bucketname=?"
	args := []interface{}{versionStatus, bucketName}

	_, err := t.Client.Exec(sql, args...)
	if err != nil {
		log.Error("error in updating versioning information, err:%v\n", err)
		return handleDBError(err)
	}

	return nil
}

func (t *TidbClient) CreateBucketVersioning(ctx context.Context, bucketName string, versionStatus string) error {
	log.Infof("create bucket[%s] Version info[%s] into tidb ...\n", bucketName, versionStatus)

	sql := "insert into bucket_versionopts(bucketname, versionstatus) values(?,?);"
	args := []interface{}{bucketName, versionStatus}

	_, err := t.Client.Exec(sql, args...)
	if err != nil {
		log.Error("error in creating versioning information, err:%v\n", err)
		return handleDBError(err)
	}

	return nil
}

func (t *TidbClient) GetBucketVersioning(ctx context.Context, bucketName string) (versionOptsPtr *pb.BucketVersioning, err error) {
	log.Info("list bucket Versions info from tidb ...")
	/*m := bson.M{}
	err = UpdateContextFilter(ctx, m)
	if err != nil {
		return nil, ErrInternalError
	}*/

	var rows *sql.Rows
	sqltext := "select versionstatus from bucket_versionopts where bucketname=?;"

	rows, err = t.Client.Query(sqltext, bucketName)

	if err == sql.ErrNoRows {
		err = nil
		return
	} else if err != nil {
		log.Error("error in getting bucket versioning configuration, err:%v\n", err)
		err = handleDBError(err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		tmp := &pb.BucketVersioning{}
		err = rows.Scan(
			&tmp.Status)
		if err != nil {
			err = handleDBError(err)
			return
		}
		return tmp, nil
	}
	return
}

func (t *TidbClient) CreateBucketSSE(ctx context.Context, bucketName string, sseType string, sseKey []byte) error {
	log.Infof("create bucket[%s] SSE info[%s] into tidb ...\n", bucketName, sseType)

	sql := "insert into bucket_sseopts(bucketname, sse, sseserverkey) values(?,?,?);"
	args := []interface{}{bucketName, sseType, sseKey}

	_, err := t.Client.Exec(sql, args...)
	if err != nil {
		return handleDBError(err)
	}

	return nil
}

func (t *TidbClient) GetBucketSSE(ctx context.Context, bucketName string) (sseOptsPtr *pb.ServerSideEncryption, err error) {
	log.Info("list bucket SSE info from tidb ...")

	var rows *sql.Rows
	sqltext := "select sse,sseserverkey from bucket_sseopts where bucketname=?;"

	rows, err = t.Client.Query(sqltext, bucketName)

	if err == sql.ErrNoRows {
		err = nil
		return
	} else if err != nil {
		err = handleDBError(err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		tmp := &pb.ServerSideEncryption{}

		err = rows.Scan(
			&tmp.SseType,
			&tmp.EncryptionKey)
		if err != nil {
			err = handleDBError(err)
			return
		}
		rErr := rows.Err()
		if rErr != nil {
			rErr = handleDBError(rErr)
			return
		}
		return tmp, nil
	}
	return
}

func (t *TidbClient) UpdateBucketSSE(ctx context.Context, bucketName string, sseType string, sseKey []byte) error {
	log.Infof("put bucket[%s] SSE info[%s] into tidb ...\n", bucketName, sseType)

	sql := "update bucket_sseopts set sse=?,sseserverkey=? where bucketname=?"
	args := []interface{}{sseType, sseKey, bucketName}

	_, err := t.Client.Exec(sql, args...)
	if err != nil {
		return handleDBError(err)
	}

	return nil
}
