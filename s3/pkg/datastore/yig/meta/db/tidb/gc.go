package tidb

import (
	"database/sql"
	"time"

	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/meta/types"
	mtypes "github.com/opensds/multi-cloud/s3/pkg/meta/types"
	log "github.com/sirupsen/logrus"
)

func (t *Tidb) PutPartsInGc(parts []*types.PartInfo) (err error) {
	var tx *sql.Tx
	var stmt *sql.Stmt
	tx, err = t.DB.Begin()
	if err != nil {
		log.Errorf("PutPartsInGc(%v) failed, err: %v", parts, err)
		return err
	}

	defer func() {
		if err == nil {
			if err = tx.Commit(); err != nil {
				log.Errorf("PutPartsInGc(%v) failed, failed to commit, err: %v", parts, err)
			}
			return
		}
		if rErr := tx.Rollback(); rErr != nil {
			log.Errorf("PutPartsInGc(%v) failed, failed to rollback, err: %v", parts, err)
		}
	}()

	// put all the parts into gc.
	stmt, err = tx.Prepare("insert into gc(location, pool, object_id) values(?, ?, ?)")
	if err != nil {
		log.Errorf("PutPartsInGc(%v) failed, failed to prepare insert to gc, err: %v", parts, err)
		return err
	}
	for _, p := range parts {
		_, err = stmt.Exec(p.Location, p.Pool, p.ObjectId)
		if err != nil {
			log.Errorf("PutPartsInGc(%v) failed, failed to exec insert gc stmt(%v), err: %v", parts, p, err)
			stmt.Close()
			return err
		}
	}
	stmt.Close()
	// remove all the parts from multiparts
	stmt, err = tx.Prepare("delete from multiparts where upload_id=? and part_num=?")
	if err != nil {
		log.Errorf("PutPartsInGc(%v) failed, failed to prepare to remove multiparts, err: %v", parts, err)
		return err
	}
	for _, p := range parts {
		_, err = stmt.Exec(p.UploadId, p.PartNum)
		if err != nil {
			log.Errorf("PutPartsInGc(%v) failed, failed to exec remove multiparts stmt(%v), err: %v", parts, p, err)
			stmt.Close()
			return err
		}
	}
	stmt.Close()
	return nil
}

// delete objects
func (t *Tidb) PutGcObjects(objects ...*types.GcObject) (err error) {
	var tx *sql.Tx
	var stmt *sql.Stmt
	tx, err = t.DB.Begin()
	if err != nil {
		log.Errorf("PutGcObjects(%v) failed, err: %v", objects, err)
		return err
	}

	defer func() {
		if err == nil {
			if err = tx.Commit(); err != nil {
				log.Errorf("PutGcObjects(%v) failed, failed to commit, err: %v", objects, err)
			}
			return
		}
		if rErr := tx.Rollback(); rErr != nil {
			log.Errorf("PutGcObjects(%v) failed, failed to rollback, err: %v", objects, err)
		}
	}()

	stmt, err = tx.Prepare("insert into gc(location, pool, object_id) values(?, ?, ?)")
	if err != nil {
		log.Errorf("PutGcObjects(%v) failed, failed to prepare, err: %v", objects, err)
		return err
	}
	defer stmt.Close()
	for _, o := range objects {
		_, err = stmt.Exec(o.Location, o.Pool, o.ObjectId)
		if err != nil {
			log.Errorf("PutGcObjects(%v) failed, failed to exec(%v), err: %v", objects, o, err)
			return err
		}
	}
	return nil
}

func (t *Tidb) GetGcObjects(marker int64, limit int) ([]*types.GcObject, error) {
	sqlText := "select id, location, pool, object_id, create_time from gc where id>=? order by create_time limit ?"
	var out []*types.GcObject
	rows, err := t.DB.Query(sqlText, marker, limit)
	if err != nil {
		log.Errorf("failed to GetGcObjects(%d, %d), err: %v", marker, limit, err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var createTime sql.NullString
		o := &types.GcObject{}
		err = rows.Scan(&o.Id, &o.Location, &o.Pool, &o.ObjectId, &createTime)
		if err != nil {
			log.Errorf("GetGcObjects(%d, %d) failed, failed to perform scan, err: %v", marker, limit, err)
			return nil, err
		}
		if createTime.Valid {
			o.CreateTime, err = time.Parse(mtypes.TIME_LAYOUT_TIDB, createTime.String)
			if err != nil {
				log.Errorf("GetGcObjects(%d, %d) failed, failed to parse create_time: %s, err: %v", marker, limit, createTime.String, err)
				return nil, err
			}
		}
		out = append(out, o)
	}
	if err = rows.Err(); err != nil {
		log.Errorf("GetGcObjects(%d, %d) failed, rows return error: %v", marker, limit, err)
		return nil, err
	}

	return out, nil
}

// delete gc objects meta.
func (t *Tidb) DeleteGcObjects(objects ...*types.GcObject) (err error) {
	var tx *sql.Tx
	var stmt *sql.Stmt
	tx, err = t.DB.Begin()
	if err != nil {
		log.Errorf("DeleteGcObjects(%v) failed, err: %v", objects, err)
		return err
	}

	defer func() {
		if err == nil {
			if err = tx.Commit(); err != nil {
				log.Errorf("DeleteGcObjects(%v) failed, failed to commit, err: %v", objects, err)
			}
			return
		}
		if rErr := tx.Rollback(); rErr != nil {
			log.Errorf("DeleteGcObjects(%v) failed, failed to rollback, err: %v", objects, err)
		}
	}()

	stmt, err = tx.Prepare("delete from gc where object_id=?")
	if err != nil {
		log.Errorf("DeleteGcObjects(%v) failed, failed to prepare, err: %v", objects, err)
		return err
	}
	defer stmt.Close()
	for _, o := range objects {
		_, err = stmt.Exec(o.ObjectId)
		if err != nil {
			log.Errorf("DeleteGcObjects(%v) failed, failed to exec(%v), err: %v", objects, o, err)
			return err
		}
	}
	return nil
}
