package lifecycle

import (
	"fmt"

	flowtype "github.com/opensds/multi-cloud/dataflow/pkg/model"
	"github.com/opensds/multi-cloud/datamover/pkg/amazon/s3"
	"github.com/opensds/multi-cloud/datamover/pkg/ceph/s3"
	"github.com/opensds/multi-cloud/datamover/pkg/huawei/obs"
	"github.com/opensds/multi-cloud/datamover/pkg/ibm/cos"
	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
	"github.com/opensds/multi-cloud/datamover/proto"
	log "github.com/sirupsen/logrus"
)

func clearFromBackend(objKey, uploadId string, loca *LocationInfo) error {
	if loca.VirBucket != "" {
		objKey = loca.VirBucket + "/" + objKey
	}

	var err error = nil
	switch loca.StorType {
	case flowtype.STOR_TYPE_AWS_S3:
		mover := s3mover.S3Mover{}
		err = mover.DeleteIncompleteMultipartUpload(objKey, uploadId, loca)
	case flowtype.STOR_TYPE_IBM_COS:
		mover := ibmcosmover.IBMCOSMover{}
		err = mover.DeleteIncompleteMultipartUpload(objKey, uploadId, loca)
	case flowtype.STOR_TYPE_HW_OBS, flowtype.STOR_TYPE_HW_FUSIONSTORAGE, flowtype.STOR_TYPE_HW_FUSIONCLOUD:
		mover := obsmover.ObsMover{}
		err = mover.DeleteIncompleteMultipartUpload(objKey, uploadId, loca)
	case flowtype.STOR_TYPE_CEPH_S3:
		mover := cephs3mover.CephS3Mover{}
		err = mover.DeleteIncompleteMultipartUpload(objKey, uploadId, loca)
	default:
		err = fmt.Errorf("delete incomplete multipart upload is not support for storage type:%s", loca.StorType)
	}

	if err != nil {
		log.Errorf("delete incomplete multipart upload[id=%s] from backend[type:%s,bucket:%s] failed.\n",
			uploadId, loca.StorType, loca.BucketName)
	} else {
		log.Infof("delete incomplete multipart upload[id=%s] from backend[type:%s,bucket:%s] successfully.\n",
			uploadId, loca.StorType, loca.BucketName)
	}

	return err
}

func doAbortUpload(acReq *datamover.LifecycleActionRequest) error {
	/*log.Infof("abort incomplete upload: key=%s, uploadid=%s.\n", acReq.ObjKey, acReq.UploadId)

	// delete incomplete multipart upload data in each backend
	bkend, err := getBackendInfo(&acReq.TargetBackend, false)
	if err != nil {
		log.Errorf("abort incomplete upload[key=%s, uploadid=%s] failed because get location failed.\n", acReq.ObjKey, acReq.UploadId)
		return err
	}

	loc := &LocationInfo{StorType: bkend.StorType, Region: bkend.Region, EndPoint: bkend.EndPoint, BucketName: bkend.BucketName,
		VirBucket: acReq.BucketName, Access: bkend.Access, Security: bkend.Security, BakendName: bkend.BakendName}
	err = clearFromBackend(acReq.ObjKey, acReq.UploadId, loc)

	// Delete record from database, if delete failed, it will be deleted again in the next schedule period
	if err == nil {
		record := osdss3.MultipartUploadRecord{ObjectKey: acReq.ObjKey, Bucket: acReq.BucketName, Backend: acReq.TargetBackend,
			UploadId: acReq.UploadId}
		s3client.DeleteUploadRecord(context.Background(), &record)
	}*/

	return nil
}
