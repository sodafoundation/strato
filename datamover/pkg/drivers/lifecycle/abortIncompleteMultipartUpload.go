package lifecycle

import (
	"context"
	"strconv"
	"time"

	"github.com/micro/go-micro/v2/metadata"
	log "github.com/sirupsen/logrus"

	"github.com/soda/multi-cloud/api/pkg/common"
	datamover "github.com/soda/multi-cloud/datamover/proto"
	osdss3 "github.com/soda/multi-cloud/s3/proto"
)

func doAbortUpload(acReq *datamover.LifecycleActionRequest) error {
	log.Infof("abort incomplete upload: key=%s, uploadid=%s.\n", acReq.ObjKey, acReq.UploadId)

	req := &osdss3.AbortMultipartRequest{BucketName: acReq.BucketName, ObjectKey: acReq.ObjKey, UploadId: acReq.UploadId}
	// as abort multipart upload does not need to move data, so it's timeout time is not need to be too large.
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	ctx = metadata.NewContext(ctx, map[string]string{common.CTX_KEY_IS_ADMIN: strconv.FormatBool(true)})
	_, err := s3client.AbortMultipartUpload(ctx, req)
	if err != nil {
		// if it failed this time, it will be aborted again in the next schedule round
		log.Warnf("abort incomplete upload failed, objKey:%s, uploadid:%s, err:%v\n", acReq.ObjKey, acReq.UploadId, err)
	}

	return nil
}
