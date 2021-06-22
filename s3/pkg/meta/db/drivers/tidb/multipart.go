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

	log "github.com/sirupsen/logrus"

	. "github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/pkg/helper"
	. "github.com/opensds/multi-cloud/s3/pkg/meta/types"
	pb "github.com/opensds/multi-cloud/s3/proto"
)

func (t *TidbClient) CreateMultipart(multipart Multipart) (err error) {
	m := multipart.Metadata
	uploadTime := multipart.InitialTime.Format(TIME_LAYOUT_TIDB)
	acl, _ := json.Marshal(m.Acl)
	attrs, _ := json.Marshal(m.Attrs)
	sqltext := "insert into multiparts(bucketname,objectname,uploadid,uploadtime,initiatorid,tenantid,userid,contenttype, " +
		"location,acl,attrs,objectid,storageMeta,storageclass) " +
		"values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
	_, err = t.Client.Exec(sqltext, multipart.BucketName, multipart.ObjectKey, multipart.UploadId, uploadTime,
		m.InitiatorId, m.TenantId, m.UserId, m.ContentType,
		m.Location, acl, attrs, multipart.ObjectId, multipart.StorageMeta, m.Tier)
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
		&multipart.Metadata.Tier,
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

	sqltext = "select partnumber,size,objectid,offset,etag,lastmodified from objectparts " +
		"where bucketname=? and objectname=? and uploadid=?;"
	rows, err := t.Client.Query(sqltext, bucketName, objectName, uploadId)
	if err != nil {
		return
	}
	defer rows.Close()
	multipart.Parts = make(map[int]*Part)
	for rows.Next() {
		p := &Part{}
		err = rows.Scan(
			&p.PartNumber,
			&p.Size,
			&p.ObjectId,
			&p.Offset,
			&p.Etag,
			&p.LastModified,
		)
		ts, e := time.Parse(TIME_LAYOUT_TIDB, p.LastModified)
		if e != nil {
			return
		}
		p.LastModified = ts.Format(CREATE_TIME_LAYOUT)
		multipart.Parts[p.PartNumber] = p
		if err != nil {
			return
		}
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

	sqltext = "delete from objectparts where bucketname=? and objectname=? and uploadid=?;"
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
	objectMap := make(map[string]struct{})
	uploadIdMap := make(map[string]struct{})
	currentMarker := keyMarker
	currentUploadIdMarker := uploadIdMarker
	for !exit {
		var loopnum int
		var sqltext string
		var rows *sql.Rows
		args := make([]interface{}, 0)
		sqltext = "select objectname,uploadid,uploadtime,initiatorid,tenantid,userid from multiparts where bucketName=?"
		args = append(args, bucketName)
		if prefix != "" {
			sqltext += " and objectname like ?"
			args = append(args, prefix+"%")
			log.Infof("query prefix: %s", prefix)
		}
		if currentMarker != "" {
			sqltext += " and objectname >= ?"
			args = append(args, currentMarker)
			log.Infof("query object name marker: %s", currentMarker)
		}
		if uploadIdMarker != "" {
			sqltext += " and uploadid >= ?"
			args = append(args, uploadIdMarker)
			log.Infof("query uplaodid marker: %s", uploadIdMarker)
		}
		if delimiter == "" {
			sqltext += " order by bucketname,objectname,uploadid limit ?"
			args = append(args, maxUploads)
		} else {
			num := len(strings.Split(prefix, delimiter))
			args = append(args, delimiter, num, maxUploads)
			sqltext += " group by SUBSTRING_INDEX(name, ?, ?) order by bucketname,objectname,uploadid limit ?"
		}
		rows, err = t.Client.Query(sqltext, args...)
		if err != nil {
			err = handleDBError(err)
			return
		}
		defer rows.Close()
		for rows.Next() {
			// loopnum is used to calculate the total number of the query result
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
				err = handleDBError(err)
				log.Errorln("sql err:", err)
				return
			}
			log.Infoln("objectName:", objName, " uploadId:", uploadId)
			// if update marker for next query
			currentMarker = objName
			currentUploadIdMarker = uploadId

			// if the object name and upload id have not been processed, we use objectMap and uploadIdMap to flag them
			var objExist, uploadIdExist bool
			if _, objExist := objectMap[objName]; !objExist {
				objectMap[objName] = struct{}{}
			}
			if _, uploadIdExist := uploadIdMap[uploadId]; !uploadIdExist {
				uploadIdMap[uploadId] = struct{}{}
			}
			// if the same record row is processed before, we should skip to next row
			if objExist && uploadIdExist {
				continue
			}

			if currentMarker == keyMarker && currentUploadIdMarker == uploadIdMarker {
				// because we search result from maker and the result should not include origin marker from client.
				continue
			}

			// filter by delimiter
			if len(delimiter) != 0 {
				subStr := strings.TrimPrefix(objName, prefix)
				n := strings.Index(subStr, delimiter)
				if n != -1 {
					prefixKey := prefix + string([]byte(subStr)[0:(n+1)])
					// the example is from https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListMultipartUploads.html
					// some multipart object key as follows:
					// photos/2006/January/sample.jpg
					// photos/2006/February/sample.jpg
					// photos/2006/March/sample.jpg
					// videos/2006/March/sample.wmv
					// sample.jpg
					// we get "photos" as prefix, and "photos0" as marker for next query. The value of character '0' is
					// next to the value of "/", and next query we should use it as marker
					currentMarker = prefixKey[0:(len(prefixKey)-1)] + string(delimiter[len(delimiter)-1]+1)
					if prefixKey == keyMarker {
						// because we search result from maker and the result should not include origin marker from client.
						continue
					}
					if _, ok := commonPrefixes[prefixKey]; !ok {
						if count == maxUploads {
							output.IsTruncated = true
							exit = true
							break
						}
						commonPrefixes[prefixKey] = struct{}{}
						output.NextKeyMarker = objName
						output.NextUploadIdMarker = uploadId
						count += 1
					}
					continue
				}
			}
			// now start to get out multipart upload records, and update nextmarker if necessary
			count += 1
			if count == maxUploads {
				output.NextKeyMarker = objName
				output.NextUploadIdMarker = uploadId
			}
			// if count are more than maxUploads, it means that we should exit the loop and set IsTruncated true
			if count > maxUploads {
				output.IsTruncated = true
				exit = true
				break
			}

			var ct time.Time
			ct, err = time.Parse(TIME_LAYOUT_TIDB, uploadTime)
			if err != nil {
				return
			}
			output.Uploads = append(output.Uploads, &pb.Upload{
				Key:      objName,
				UploadId: uploadId,
				Initiator: &pb.Owner{
					Id:          tenantId,
					DisplayName: tenantId,
				},
				Owner: &pb.Owner{
					Id:          initiatorId,
					DisplayName: initiatorId,
				},
				Initiated:    ct.Format(CREATE_TIME_LAYOUT),
				StorageClass: "STANDARD",
			})
		}

		// if the number of query result are less maxUploads, it must be no more result, and we can exit the loop
		if loopnum < maxUploads {
			exit = true
		}
	}
	output.CommonPrefix = helper.Keys(commonPrefixes)
	return
}

func (t *TidbClient) PutObjectPart(multipart *Multipart, part *Part, tx interface{}) (err error) {
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

	lastt, err := time.Parse(CREATE_TIME_LAYOUT, part.LastModified)
	if err != nil {
		return
	}
	lastModified := lastt.Format(TIME_LAYOUT_TIDB)
	sqltext := "insert into objectparts(bucketname,objectname,uploadid,partnumber,size,objectid,offset,etag,lastmodified) " +
		"values(?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE size=?,objectid=?,offset=?,etag=?,lastmodified=?"
	_, err = sqlTx.Exec(sqltext, multipart.BucketName, multipart.ObjectKey, multipart.UploadId, part.PartNumber, part.Size,
		part.ObjectId, part.Offset, part.Etag, lastModified, part.Size, part.ObjectId, part.Offset, part.Etag, lastModified)
	return
}
