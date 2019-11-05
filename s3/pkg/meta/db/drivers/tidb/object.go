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

	. "github.com/opensds/multi-cloud/s3/pkg/meta/types"
	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
	"github.com/xxtea/xxtea-go/xxtea"
)

func (t *TidbClient) GetObject(ctx context.Context, bucketName, objectName, version string) (object *Object, err error) {
	var sqltext, ibucketname, iname, customattributes, acl, lastModified string
	var iversion uint64
	var row *sql.Row
	if version == "" {
		sqltext = "select * from objects where bucketname=? and name=? order by bucketname,name,version limit 1;"
		row = t.Client.QueryRow(sqltext, bucketName, objectName)
	} else {
		sqltext = "select * from objects where bucketname=? and name=? and version=?;"
		row = t.Client.QueryRow(sqltext, bucketName, objectName, version)
	}
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
	)
	if err != nil {
		log.Errorf("err: %v\n", err)
		err = handleDBError(err)
		return
	}
	object.GetRowkey()
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
	/*object.Parts, err = getParts(object.BucketName, object.Name, iversion, t.Client)
	//build simple index for multipart
	if len(object.Parts) != 0 {
		var sortedPartNum = make([]int64, len(object.Parts))
		for k, v := range object.Parts {
			sortedPartNum[k-1] = v.Offset
		}
		object.PartsIndex = &SimpleIndex{Index: sortedPartNum}
	}*/

	timestamp := math.MaxUint64 - iversion
	timeData := []byte(strconv.FormatUint(timestamp, 10))
	object.VersionId = hex.EncodeToString(xxtea.Encrypt(timeData, XXTEA_KEY))
	return
}

/*
func (t *TidbClient) GetAllObject(bucketName, objectName, version string) (object []*Object, err error) {
	sqltext := "select version from objects where bucketname=? and name=?;"
	var versions []string
	rows, err := t.Client.Query(sqltext, bucketName, objectName)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var sversion string
		err = rows.Scan(&sversion)
		if err != nil {
			return
		}
		versions = append(versions, sversion)
	}
	for _, v := range versions {
		var obj *Object
		obj, err = t.GetObject(bucketName, objectName, v)
		if err != nil {
			return
		}
		object = append(object, obj)
	}
	return
}

func (t *TidbClient) UpdateObjectAcl(object *Object) error {
	sql, args := object.GetUpdateAclSql()
	_, err := t.Client.Exec(sql, args...)
	return err
}

func (t *TidbClient) UpdateObjectAttrs(object *Object) error {
	sql, args := object.GetUpdateAttrsSql()
	_, err := t.Client.Exec(sql, args...)
	return err
}

func (t *TidbClient) UpdateAppendObject(o *Object) (err error) {
	sql, args := o.GetAppendSql()
	_, err = t.Client.Exec(sql, args...)
	return err
}
*/

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
	/*if object.Parts != nil {
		v := math.MaxUint64 - uint64(object.LastModified)
		version := strconv.FormatUint(v, 10)
		for _, p := range object.Parts {
			psql, args := p.GetCreateSql(object.BucketName, object.ObjectKey, version)
			_, err = sqlTx.Exec(psql, args...)
			if err != nil {
				return err
			}
		}
	}*/
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

	vidByte, _ := hex.DecodeString(object.VersionId)
	decrByte := xxtea.Decrypt(vidByte, XXTEA_KEY)
	reVersion, _ := strconv.ParseUint(string(decrByte), 10, 64)
	version := math.MaxUint64 - reVersion
	log.Infof("delete from objects where name=%s and bucketname=%s and version=%d;\n",
		object.ObjectKey, object.BucketName, version)

	sqltext := "delete from objects where name=? and bucketname=? and version=?;"
	_, err = sqlTx.Exec(sqltext, object.ObjectKey, object.BucketName, version)
	if err != nil {
		return err
	}
	sqltext = "delete from objectpart where objectname=? and bucketname=? and version=?;"
	_, err = sqlTx.Exec(sqltext, object.ObjectKey, object.BucketName, version)
	if err != nil {
		return err
	}
	return nil
}

//util function
/*func getParts(bucketName, objectName string, version uint64, cli *sql.DB) (parts map[int]*Part, err error) {
	parts = make(map[int]*Part)
	sqltext := "select partnumber,size,objectid,offset,etag,lastmodified,initializationvector from objectpart where bucketname=? and objectname=? and version=?;"
	rows, err := cli.Query(sqltext, bucketName, objectName, version)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var p *Part = &Part{}
		err = rows.Scan(
			&p.PartNumber,
			&p.Size,
			&p.ObjectId,
			&p.Offset,
			&p.Etag,
			&p.LastModified,
			&p.InitializationVector,
		)
		parts[p.PartNumber] = p
	}
	return
}*/
