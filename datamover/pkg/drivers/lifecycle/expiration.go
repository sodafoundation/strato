// Copyright 2019 The OpenSDS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lifecycle

import (
	"context"
	"errors"

	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
	datamover "github.com/opensds/multi-cloud/datamover/proto"
	osdss3 "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

func deleteObj(objKey string, lastmodifed int64, virtBucket string, bkend *BackendInfo) error {
	log.Infof("object expiration: objKey=%s, virtBucket=%s, bkend:%+v\n", objKey, virtBucket, *bkend)
	if virtBucket == "" {
		log.Infof("expiration of object[%s] is failed: virtual bucket is null.\n", objKey)
		return errors.New(DMERR_InternalError)
	}

	loca := &LocationInfo{StorType: bkend.StorType, Region: bkend.Region, EndPoint: bkend.EndPoint, BucketName: bkend.BucketName,
		Access: bkend.Access, Security: bkend.Security, BakendName: bkend.BakendName, VirBucket: virtBucket}
	err := deleteObjFromBackend(objKey, loca)
	if err != nil {
		return err
	}

	// delete metadata
	delMetaReq := osdss3.DeleteObjectInput{Bucket: virtBucket, Key: objKey, Lastmodified: lastmodifed}
	ctx := context.Background()
	_, err = s3client.DeleteObject(ctx, &delMetaReq)
	if err != nil {
		// if it is deleted failed, it will be delete again in the next schedule round
		log.Errorf("delete object metadata of obj[bucket:%s,objKey:%s] failed, err:%v\n",
			virtBucket, objKey, err)
		return err
	} else {
		log.Infof("delete object metadata of obj[bucket:%s,objKey:%s] successfully.\n",
			virtBucket, objKey)
	}

	return err
}

func doExpirationAction(acReq *datamover.LifecycleActionRequest) error {
	log.Infof("delete action: delete %s.\n", acReq.ObjKey)

	loc, err := getBackendInfo(&acReq.SourceBackend, false)
	if err != nil {
		log.Errorf("expiration of %s failed because get location failed\n", acReq.ObjKey)
		return err
	}

	err = deleteObj(acReq.ObjKey, acReq.LastModified, acReq.BucketName, loc)
	if err != nil && err.Error() == DMERR_NoPermission {
		// if permission denied, then flash backend information and try again
		loc, err = getBackendInfo(&acReq.SourceBackend, true)
		if err != nil {
			return err
		}
		err = deleteObj(acReq.ObjKey, acReq.LastModified, acReq.BucketName, loc)
	}

	return err
}
