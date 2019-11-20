package meta

import (
	"context"
	"github.com/opensds/multi-cloud/s3/pkg/meta/types"
	log "github.com/sirupsen/logrus"
)

func (m *Meta) AddGcobjRecord(ctx context.Context, obj *types.Object) error {
	return m.Db.PutGcobjRecord(ctx, obj, nil)
}

func (m *Meta) DeleteGcobjRecord(ctx context.Context, obj *types.Object) error {
	return m.Db.DeleteGcobjRecord(ctx, obj, nil)
}

func (m *Meta) UpdateMetaAfterCopy(ctx context.Context, old, new *types.Object) error {
	tx, err := m.Db.NewTrans()
	defer func() {
		if err != nil {
			m.Db.AbortTrans(tx)
		}
	}()

	// delete new object from gc
	err = m.Db.DeleteGcobjRecord(ctx, new, tx)
	if err != nil {
		log.Errorf("err: %v\n", err)
		return err
	}

	// update object metadata to be new
	err = m.Db.UpdateObject(ctx, old, new, tx)
	if err != nil {
		log.Errorf("err: %v\n", err)
		return err
	}

	// add old object for gc
	err = m.Db.PutGcobjRecord(ctx, old, tx)
	if err != nil {
		log.Errorf("err: %v\n", err)
		return err
	}

	// commit transaction
	err = m.Db.CommitTrans(tx)
	if err != nil {
		log.Errorf("err: %v\n", err)
	}

	return err
}
