package meta

import (
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/meta/types"
)

// delete multipart uploaded part objects and put them into gc
func (m *Meta) PutPartsInGc(parts []*types.PartInfo) error {
	return m.db.PutPartsInGc(parts)
}

func (m *Meta) PutGcObjects(objects ...*types.GcObject) error {
	return m.db.PutGcObjects(objects...)
}

// get gc objects by marker and limit
func (m *Meta) GetGcObjects(marker int64, limit int) ([]*types.GcObject, error) {
	return m.db.GetGcObjects(marker, limit)
}

// delete gc objects meta.
func (m *Meta) DeleteGcObjects(objects ...*types.GcObject) error {
	return m.db.DeleteGcObjects(objects...)
}
