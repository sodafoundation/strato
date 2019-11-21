package common

import (
	"context"
	"github.com/opensds/multi-cloud/s3/error"
	log "github.com/sirupsen/logrus"
)

func GetSizeAndMd5FromCtx(ctx context.Context) (size int64, md5val string, err error) {
	// get size from context.
	val := ctx.Value(CONTEXT_KEY_SIZE)
	if val == nil {
		err = s3error.ErrIncompleteBody
		log.Errorln("no size provided")
		return
	}
	size = val.(int64)

	// md5 provided by user for uploading object.
	if val = ctx.Value(CONTEXT_KEY_MD5); val != nil {
		md5val = val.(string)
	}

	return
}
