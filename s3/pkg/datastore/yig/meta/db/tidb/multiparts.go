package tidb

import (
	"database/sql"
	"sort"
	"time"

	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/meta/types"
	mtypes "github.com/opensds/multi-cloud/s3/pkg/meta/types"
	log "github.com/sirupsen/logrus"
)

/*
* ListParts: list all the parts which belong to the uploadId.
* @return: the related parts which are sorted by PartNum asc.
*
 */
func (t *Tidb) ListParts(uploadId uint64) ([]*types.PartInfo, error) {
	sqlText := "select upload_id, part_num, object_id, location, pool, offset, size, etag, flag, create_time, update_time from multiparts where upload_id = ? order by part_num"
	var parts []*types.PartInfo
	rows, err := t.DB.Query(sqlText, uploadId)
	if err != nil {
		log.Errorf("failed to query parts for uploadId %d, err: %v", uploadId, err)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		part := &types.PartInfo{}
		var createTime sql.NullString
		var updateTime sql.NullString
		err = rows.Scan(&part.UploadId, &part.PartNum, &part.ObjectId, &part.Location, &part.Pool, &part.Offset, &part.Size, &part.Etag, &part.Flag, &createTime, &updateTime)
		if err != nil {
			log.Errorf("failed to scan rows for uploadId %d, err: %v", uploadId, err)
			return nil, err
		}
		if createTime.Valid {
			part.CreateTime, err = time.Parse(mtypes.TIME_LAYOUT_TIDB, createTime.String)
			if err != nil {
				log.Errorf("failed to parse create_time: %s, err: %v", createTime.String, err)
				return nil, err
			}
		}
		if updateTime.Valid {
			part.UpdateTime, err = time.Parse(mtypes.TIME_LAYOUT_TIDB, updateTime.String)
			if err != nil {
				log.Errorf("failed to parse update_time: %s, err: %v", updateTime.String, err)
				return nil, err
			}
		}
		parts = append(parts, part)
	}
	err = rows.Err()
	if err != nil {
		log.Errorf("failed to iterate the rows for uploadId %d, err: %v", uploadId, err)
		return nil, err
	}
	sort.Sort(types.ByPartNum(parts))
	return parts, nil
}

func (t *Tidb) PutPart(partInfo *types.PartInfo) (err error) {
	sqlText := "insert into multiparts(upload_id, part_num, object_id, location, pool, offset, size, etag, flag) values(?, ?, ?, ?, ?, ?, ?, ?, ?)"
	var tx *sql.Tx
	tx, err = t.DB.Begin()
	if err != nil {
		log.Errorf("failed to Begin a transaction for %v, err: %v", partInfo, err)
		return err
	}

	defer func() {
		if err != nil {
			// do not use the err since since by doing so, it will overwite the original error.
			if rErr := tx.Rollback(); rErr != nil {
				log.Errorf("perform rollback for partInfo(%v) failed with err: %v", partInfo, rErr)
			}
		} else {
			// should check whether the transaction commit succeeds or not.
			if err = tx.Commit(); err != nil {
				log.Errorf("perform commit for partInfo(%v) failed with err: %v", partInfo, err)
			}
		}
	}()

	_, err = tx.Exec(sqlText, partInfo.UploadId, partInfo.PartNum, partInfo.ObjectId, partInfo.Location, partInfo.Pool, partInfo.Offset, partInfo.Size, partInfo.Etag, partInfo.Flag)
	if err != nil {
		log.Errorf("failed to save partInfo(%v), err: %v", partInfo, err)
		return err
	}
	// must return err instead of nil, because commit may return error.
	return err
}

func (t *Tidb) DeleteParts(uploadId uint64) error {
	sqlText := "delete from multiparts where upload_id=?"
	_, err := t.DB.Exec(sqlText, uploadId)
	if err != nil {
		log.Errorf("failed to remove parts from meta for uploadId(%d), err: %v", uploadId, err)
		return err
	}
	return nil
}

func (t *Tidb) CompleteParts(uploadId uint64, parts []*types.PartInfo) (err error) {
	sqlText := "update multiparts set offset=?, flag=? where upload_id=? and part_num=?"
	var tx *sql.Tx
	var stmt *sql.Stmt

	tx, err = t.DB.Begin()
	if err != nil {
		log.Errorf("failed to complete parts for uploadId(%d), it was fail to create transaction, err: %v", uploadId, err)
		return err
	}

	defer func() {
		if err == nil {
			if err = tx.Commit(); err != nil {
				log.Errorf("failed to commit tranaction when completing uploadId(%d), err: %v", uploadId, err)
			}
		} else {
			if rErr := tx.Rollback(); rErr != nil {
				log.Errorf("failed to rollback when completing uploadId(%d), err: %v", uploadId, rErr)
			}
		}
	}()

	stmt, err = tx.Prepare(sqlText)
	if err != nil {
		log.Errorf("failed to complete uploadId(%d), it was fail to prepare sql, err: %v", uploadId, err)
		return err
	}

	defer stmt.Close()

	for _, part := range parts {
		_, err := stmt.Exec(part.Offset, part.Flag, uploadId, part.PartNum)
		if err != nil {
			log.Errorf("failed to complete uploadId(%d), it was fail to perform stmt exec, err: %v", uploadId, err)
			return err
		}
	}

	return err
}
