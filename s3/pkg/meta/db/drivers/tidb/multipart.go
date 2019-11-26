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
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	. "github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/pkg/helper"
	. "github.com/opensds/multi-cloud/s3/pkg/meta/types"
	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

func (t *TidbClient) CreateMultipart(multipart Multipart) (err error) {
	m := multipart.Metadata
	uploadTime := multipart.InitialTime.Format(TIME_LAYOUT_TIDB)
	acl, _ := json.Marshal(m.Acl)
	attrs, _ := json.Marshal(m.Attrs)
	sqltext := "insert into multiparts(bucketname,objectname,uploadid,uploadtime,initiatorid,tenantid,userid,contenttype, " +
		"location,acl,attrs,objectid,storageMeta,storageclass) " +
		"values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	_, err = t.Client.Exec(sqltext, multipart.BucketName, multipart.ObjectKey, multipart.UploadId,uploadTime,
		m.InitiatorId, m.TenantId, m.UserId, m.ContentType,
		m.Location, acl, attrs, multipart.ObjectId, multipart.StorageMeta, m.StorageClass)
	return
}

func (t *TidbClient) GetMultipart(bucketName, objectName, uploadId string) (multipart Multipart, err error) {
	sqltext := "select bucketname,objectname,uploadid,uploadtime,initiatorid,tenantid,userid,contenttype," +
		"location,acl,attrs,objectid,storageMeta,storageclass from multiparts " +
		"where bucketname=? and objectname=? and uploadid=?;"
	var acl, attrs, uploadTime string
	err = t.Client.QueryRow(sqltext, bucketName, objectName, uploadId).Scan(
		&multipart.BucketName,
		&multipart.ObjectKey,
		&multipart.UploadId,
		&uploadTime,
		&multipart.Metadata.InitiatorId,
		&multipart.Metadata.TenantId,
		&multipart.Metadata.UserId,
		&multipart.Metadata.ContentType,
		&multipart.Metadata.Location,
		&acl,
		&attrs,
		&multipart.ObjectId,
		&multipart.StorageMeta,
		&multipart.Metadata.StorageClass,
	)
	if err != nil && err == sql.ErrNoRows {
		err = ErrNoSuchUpload
		return
	} else if err != nil {
		return
	}

	ut, err := time.Parse(TIME_LAYOUT_TIDB, uploadTime)
	if err != nil {
		err = handleDBError(err)
		return
	}
	multipart.InitialTime = ut

	err = json.Unmarshal([]byte(acl), &multipart.Metadata.Acl)
	if err != nil {
		return
	}

	err = json.Unmarshal([]byte(attrs), &multipart.Metadata.Attrs)
	if err != nil {
		return
	}

	return
}

func (t *TidbClient) DeleteMultipart(multipart *Multipart, tx interface{}) (err error) {
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

	sqltext := "delete from multiparts where bucketname=? and objectname=? and uploadid=?;"
	_, err = sqlTx.Exec(sqltext, multipart.BucketName, multipart.ObjectKey, multipart.UploadId)
	if err != nil {
		return
	}
	return
}

func (t *TidbClient) ListMultipartUploads(input *pb.ListBucketUploadRequest) (output *pb.ListBucketUploadResult, err error) {
	var count int
	var exit bool
	bucketName := input.BucketName
	keyMarker := input.KeyMarker
	uploadIdMarker := input.UploadIdMarker
	prefix := input.Prefix
	delimiter := input.Delimiter
	maxUploads := int(input.MaxUploads)

	output = &pb.ListBucketUploadResult{}

	log.Infof("bucketName:%v, keyMarker:%v, uploadIdMarker:%v,prefix:%v,delimiter:%v,maxUploads:%v",
		bucketName, keyMarker, uploadIdMarker, prefix, delimiter, maxUploads)

	commonPrefixes := make(map[string]struct{})
	first := true
	objNum := make(map[string]int)
	currentMarker := keyMarker
	for {
		var loopnum int
		if _, ok := objNum[currentMarker]; !ok {
			objNum[currentMarker] = 0
		}
		var sqltext string
		var rows *sql.Rows
		if currentMarker == "" {
			sqltext = "select objectname,uploadid,uploadtime,initiatorid,tenantid,userid " +
				"from multiparts where bucketName=? order by bucketname,objectname,uploadid limit ?,?;"
			rows, err = t.Client.Query(sqltext, bucketName, objNum[currentMarker], objNum[currentMarker]+maxUploads)
		} else {
			sqltext = "select objectname,uploadid,uploadtime,initiatorid,tenantid,userid " +
				"from multiparts where bucketName=? and objectname>=? order by bucketname,objectname,uploadid limit ?,?;"
			rows, err = t.Client.Query(sqltext, bucketName, currentMarker, objNum[currentMarker], objNum[currentMarker]+maxUploads)
		}
		if err != nil {
			return
		}
		log.Infoln("objNum[currentMarker]:", objNum[currentMarker])
		defer rows.Close()
		for rows.Next() {
			loopnum += 1
			var objName, uploadId, initiatorId, tenantId, uploadTime, userId string
			err = rows.Scan(
				&objName,
				&uploadId,
				&uploadTime,
				&initiatorId,
				&tenantId,
				&userId,
			)
			if err != nil {
				log.Errorln("sql err:", err)
				return
			}
			log.Infoln("objectName:", objName, " uploadId:", uploadId)
			if _, ok := objNum[objName]; !ok {
				objNum[objName] = 0
			}
			objNum[objName] += 1
			currentMarker = objName
			//filte by uploadtime and key
			if first {
				if uploadIdMarker != "" {
					if objName == currentMarker && uploadId < uploadIdMarker {
						continue
					}
				}
			}
			//filte by prefix
			hasPrefix := strings.HasPrefix(objName, prefix)
			if !hasPrefix {
				continue
			}
			//filte by delimiter
			if len(delimiter) != 0 {
				subStr := strings.TrimPrefix(objName, prefix)
				n := strings.Index(subStr, delimiter)
				if n != -1 {
					prefixKey := string([]byte(subStr)[0:(n + 1)])
					if _, ok := commonPrefixes[prefixKey]; !ok {
						commonPrefixes[prefixKey] = struct{}{}
					}
					continue
				}
			}
			if count >= maxUploads {
				output.IsTruncated = true
				output.NextKeyMarker = objName
				output.NextUploadIdMarker = uploadId
				exit = true
				break
			}
			var ct time.Time
			ct, err = time.Parse(TIME_LAYOUT_TIDB, uploadTime)
			if err != nil {
				err = handleDBError(err)
				return
			}

			output.Uploads = append(output.Uploads, &pb.Upload{
				Key:objName,
				UploadId:uploadId,
				Initiator:&pb.Owner{
					Id: tenantId,
					DisplayName:tenantId,
				},
				Owner:&pb.Owner{
					Id:initiatorId,
					DisplayName:initiatorId,
				},
				Initiated: ct.Format(CREATE_TIME_LAYOUT),
				StorageClass: "STANDARD",
			})
			count += 1
		}
		if loopnum == 0 {
			exit = true
		}
		first = false
		if exit {
			break
		}
	}
	output.CommonPrefix = helper.Keys(commonPrefixes)
	return
}
