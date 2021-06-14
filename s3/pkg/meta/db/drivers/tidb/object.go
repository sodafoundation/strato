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
	"encoding/hex"
	"encoding/json"
	"math"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/xxtea/xxtea-go/xxtea"

	. "github.com/opensds/multi-cloud/s3/pkg/meta/types"
	pb "github.com/opensds/multi-cloud/s3/proto"
)

func (t *TidbClient) GetObject(ctx context.Context, bucketName, objectName, version string) (object *Object, err error) {
	var sqltext, ibucketname, iname, customattributes, acl, lastModified string
	var iversion uint64
	var row *sql.Row
	if version == "" {
		sqltext = "select bucketname,name,version,location,tenantid,userid,size,objectid,lastmodifiedtime,etag," +
			"contenttype,customattributes,acl,nullversion,deletemarker,ssetype,encryptionkey,initializationvector,type,tier,storageMeta,encsize" +
			" from objects where bucketname=? and name=? order by bucketname,name,version limit 1;"
		row = t.Client.QueryRow(sqltext, bucketName, objectName)
	} else {
		sqltext = "select bucketname,name,version,location,tenantid,userid,size,objectid,lastmodifiedtime,etag," +
			"contenttype,customattributes,acl,nullversion,deletemarker,ssetype,encryptionkey,initializationvector,type,tier,storageMeta,encsize" +
			" from objects where bucketname=? and name=? and version=?;"
		row = t.Client.QueryRow(sqltext, bucketName, objectName, version)
	}
	log.Infof("sqltext:%s, bucketName=%s, objectName=%s, version:%s\n", sqltext, bucketName, objectName, version)
	object = &Object{Object: &pb.Object{ServerSideEncryption: &pb.ServerSideEncryption{}}}
	err = row.Scan(
		&ibucketname,
		&iname,
		&iversion,
		&object.Location,
		&object.TenantId,
		&object.UserId,
		&object.Size,
		&object.ObjectId,
		&lastModified,
		&object.Etag,
		&object.ContentType,
		&customattributes,
		&acl,
		&object.NullVersion,
		&object.DeleteMarker,
		&object.ServerSideEncryption.SseType,
		&object.ServerSideEncryption.EncryptionKey,
		&object.ServerSideEncryption.InitilizationVector,
		&object.Type,
		&object.Tier,
		&object.StorageMeta,
		&object.EncSize,
	)
	if err != nil {
		log.Errorf("err: %v\n", err)
		err = handleDBError(err)
		return
	}

	object.ObjectKey = objectName
	object.BucketName = bucketName
	lastModifiedTime, _ := time.ParseInLocation(TIME_LAYOUT_TIDB, lastModified, time.Local)
	object.LastModified = lastModifiedTime.Unix()

	err = json.Unmarshal([]byte(acl), &object.Acl)
	if err != nil {
		return
	}
	err = json.Unmarshal([]byte(customattributes), &object.CustomAttributes)
	if err != nil {
		return
	}
	// TODO: getting multi-parts
	timestamp := math.MaxUint64 - iversion
	timeData := []byte(strconv.FormatUint(timestamp, 10))
	object.VersionId = hex.EncodeToString(xxtea.Encrypt(timeData, XXTEA_KEY))
	return
}

func (t *TidbClient) UpdateObjectMeta(object *Object) error {
	sql, args := object.GetUpdateMetaSql()
	_, err := t.Client.Exec(sql, args...)
	return err
}

func (t *TidbClient) PutObject(ctx context.Context, object *Object, tx interface{}) (err error) {
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
	sql, args := object.GetCreateSql()
	_, err = sqlTx.Exec(sql, args...)
	// TODO: multi-part handle, see issue https://github.com/opensds/multi-cloud/issues/690

	return err
}

func (t *TidbClient) DeleteObject(ctx context.Context, object *Object, tx interface{}) (err error) {
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

	version := VersionStr2UInt64(object.VersionId)
	log.Infof("delete from objects where name=%s and bucketname=%s and version=%d;\n",
		object.ObjectKey, object.BucketName, version)

	sqltext := "delete from objects where name=? and bucketname=? and version=?;"
	_, err = sqlTx.Exec(sqltext, object.ObjectKey, object.BucketName, version)
	if err != nil {
		return err
	}

	return nil
}

func (t *TidbClient) SetObjectDeleteMarker(ctx context.Context, object *Object, deleteMarker bool) error {
	version := VersionStr2UInt64(object.VersionId)

	sqltext := "update objects set deletemarker=? where bucketname=? and name=? and version=?;"
	_, err := t.Client.Exec(sqltext, deleteMarker, object.BucketName, object.ObjectKey, version)
	if err != nil {
		return err
	}

	return nil
}

func (t *TidbClient) UpdateObject4Lifecycle(ctx context.Context, old, new *Object, tx interface{}) (err error) {
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

	oldversion := VersionStr2UInt64(old.VersionId)

	sqltext := "update objects set location=?,objectid=?,tier=?,storageMeta=? where bucketname=? and name=? and version=?"
	args := []interface{}{new.Location, new.ObjectId, new.Tier, new.StorageMeta, old.BucketName, old.ObjectKey, oldversion}

	log.Debugf("sqltext:%s, args:%+v\n", sqltext, args)
	_, err = sqlTx.Exec(sqltext, args...)

	log.Debugf("err:%v\n", err)
	return err
}
