package meta

import (
	"context"

	"github.com/opensds/multi-cloud/s3/pkg/meta/types"
)

func (m *Meta) AddGcobjRecord(ctx context.Context, obj *types.Object) error {
	return m.Db.PutGcobjRecord(ctx, obj, nil)
}

func (m *Meta) DeleteGcobjRecord(ctx context.Context, obj *types.Object) error {
	return m.Db.DeleteGcobjRecord(ctx, obj, nil)
}

func (m *Meta) ListGcObjs(ctx context.Context, offset, limit int) ([]*types.Object, error) {
	return m.Db.ListGcObjs(ctx, offset, limit)
}
