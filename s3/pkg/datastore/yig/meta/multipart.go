package meta

import (
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/meta/types"
)

func (m *Meta) ListParts(uploadId uint64) ([]*types.PartInfo, error) {
	return m.db.ListParts(uploadId)
}

func (m *Meta) PutPart(partInfo *types.PartInfo) error {
	return m.db.PutPart(partInfo)
}

func (m *Meta) DeleteParts(uploadId uint64) error {
	return m.db.DeleteParts(uploadId)
}

func (m *Meta) CompleteParts(uploadId uint64, parts []*types.PartInfo) error {
	return m.db.CompleteParts(uploadId, parts)
}
