package gc

import (
	"context"
	"strconv"
	"time"

	"github.com/micro/go-micro/client"
	bkd "github.com/opensds/multi-cloud/backend/proto"
	"github.com/opensds/multi-cloud/s3/pkg/meta/types"

	"github.com/micro/go-micro/metadata"
	"github.com/opensds/multi-cloud/api/pkg/common"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/driver"
	"github.com/opensds/multi-cloud/s3/pkg/meta"
	"github.com/opensds/multi-cloud/s3/pkg/utils"
	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

var CTX context.Context
var CancleFunc context.CancelFunc

const (
	LIST_LIMIT = 1000
)

func Init(ctx context.Context, cancelFunc context.CancelFunc, meta *meta.Meta) {
	mt := meta
	CTX = ctx
	CancleFunc = cancelFunc
	backend := bkd.NewBackendService("backend", client.DefaultClient)
	go Run(mt, backend)
}

func Stop() {
	CancleFunc()
}

func Run(mt *meta.Meta, bkservice bkd.BackendService) {
	for {
		select {
		case <-time.After(60 * time.Second):
		case <-CTX.Done():
			log.Infoln("gc exit...")
			return
		}

		offset := 0
		for {
			// get gc objects
			log.Debugln("list gc objects ...")
			objs, err := mt.ListGcObjs(CTX, offset, LIST_LIMIT)
			if err != nil {
				log.Warnf("list gc objects failed, err:%v\n", err)
				// try in next round
				break
			}

			total := len(objs)
			deleted := 0
			// for each obj, do clean
			for _, o := range objs {
				err = CleanFromBackend(o, bkservice)
				if err == nil {
					err = mt.DeleteGcobjRecord(CTX, o)
					if err != nil {
						// if delete failed, it will be deleted in the next round
						log.Warnf("delete gc object[key=%s,version=%s] metadata failed, err:%v\n", o.ObjectKey, o.VersionId, err)
					} else {
						deleted++
					}
				}
			}
			// if some obj deleted failed, do not try to delete it again in this round, but do it in next round
			offset += total - deleted
			log.Debugf("total=%d, deleted=%d, offset=%d\n", total, deleted, offset)

			if total < LIST_LIMIT {
				log.Debugln("break this round of gc")
				break
			}
		}
	}
}

func CleanFromBackend(obj *types.Object, bkservice bkd.BackendService) error {
	ctx := metadata.NewContext(context.Background(), map[string]string{
		common.CTX_KEY_IS_ADMIN: strconv.FormatBool(true),
	})
	backend, err := utils.GetBackend(ctx, bkservice, obj.Location)
	if err != nil {
		log.Errorf("get backend faild, err:%v\n", err)
		return err
	}

	sd, err := driver.CreateStorageDriver(backend.Type, backend)
	if err != nil {
		log.Errorf("failed to create storage driver for %s, err:%v\n", backend.Type, err)
		return err
	}

	// delete object data in backend
	log.Debugf("delete object, key=%s, verionid=%s, objectid=%s, storageMeta:%+v\n", obj.ObjectKey, obj.VersionId, obj.ObjectId, obj.StorageMeta)
	err = sd.Delete(ctx, &pb.DeleteObjectInput{Bucket: obj.BucketName, Key: obj.ObjectKey, VersioId: obj.VersionId,
		StorageMeta: obj.StorageMeta, ObjectId: obj.ObjectId})
	if err != nil {
		log.Errorf("failed to delete obejct[%s] from backend storage, err:", obj.ObjectKey, err)
	} else {
		log.Infof("delete obejct[%s] from backend storage successfully.", obj.ObjectKey)
	}

	return err
}
