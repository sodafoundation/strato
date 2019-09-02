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
	"fmt"

	log "github.com/sirupsen/logrus"
	backend "github.com/opensds/multi-cloud/backend/pkg/utils/constants"
	flowtype "github.com/opensds/multi-cloud/dataflow/pkg/model"
	"github.com/opensds/multi-cloud/datamover/pkg/amazon/s3"
	"github.com/opensds/multi-cloud/datamover/pkg/azure/blob"
	"github.com/opensds/multi-cloud/datamover/pkg/hw/obs"
	"github.com/opensds/multi-cloud/datamover/pkg/ibm/cos"
	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
	"github.com/opensds/multi-cloud/datamover/proto"
	osdss3 "github.com/opensds/multi-cloud/s3/proto"
	"sync"
)

func changeStorageClass(objKey *string, newClass *string, virtBucket *string, bkend *BackendInfo) error {
	log.Infof("Change storage class of object[%s] to %s.\n", *objKey, *newClass)
	if *virtBucket == "" {
		log.Infof("change storage class of object[%s] is failed: virtual bucket is null\n", objKey)
		return errors.New(DMERR_InternalError)
	}

	key := *objKey
	if *virtBucket != "" {
		key = *virtBucket + "/" + *objKey
	}

	var err error
	switch bkend.StorType {
	case flowtype.STOR_TYPE_AWS_S3:
		mover := s3mover.S3Mover{}
		err = mover.ChangeStorageClass(&key, newClass, bkend)
	case flowtype.STOR_TYPE_IBM_COS:
		mover := ibmcosmover.IBMCOSMover{}
		err = mover.ChangeStorageClass(&key, newClass, bkend)
	case flowtype.STOR_TYPE_HW_OBS:
		mover := obsmover.ObsMover{}
		err = mover.ChangeStorageClass(&key, newClass, bkend)
	case flowtype.STOR_TYPE_AZURE_BLOB:
		mover := blobmover.BlobMover{}
		err = mover.ChangeStorageClass(&key, newClass, bkend)
	default:
		log.Infof("change storage class of object[objkey:%s] failed: backend type is not support.\n", objKey)
		err = errors.New(DMERR_UnSupportBackendType)
	}

	if err == nil {
		log.Infof("Change storage class of object[%s] to %s successfully.\n", *objKey, *newClass)
	}

	return err
}

func loadStorageClassDefinition() error {
	res, _ := s3client.GetTierMap(context.Background(), &osdss3.BaseRequest{})
	if len(res.Tier2Name) == 0 {
		log.Info("get tier definition failed")
		return fmt.Errorf("get tier definition failed")
	}

	log.Infof("Load storage class definition from s3 service successfully, res.Tier2Name:%+v\n", res.Tier2Name)
	Int2ExtTierMap = make(map[string]*Int2String)
	for k, v := range res.Tier2Name {
		val := make(Int2String)
		for k1, v1 := range v.Lst {
			val[k1] = v1
		}
		Int2ExtTierMap[k] = &val
	}

	return nil
}

func getStorageClassName(tier int32, storageType string) (string, error) {
	log.Infof("Get storage class name of tier[%d].\n", tier)
	var err error
	var mutex sync.Mutex
	mutex.Lock()
	if len(Int2ExtTierMap) == 0 {
		err = loadStorageClassDefinition()
	} else {
		err = nil
	}
	mutex.Unlock()

	if err != nil {
		return "", err
	}

	key := ""
	switch storageType {
	case flowtype.STOR_TYPE_AWS_S3:
		key = backend.BackendTypeAws
	case flowtype.STOR_TYPE_IBM_COS:
		key = backend.BackendTypeIBMCos
	case flowtype.STOR_TYPE_HW_OBS, flowtype.STOR_TYPE_HW_FUSIONSTORAGE, flowtype.STOR_TYPE_HW_FUSIONCLOUD:
		key = backend.BackendTypeObs
	case flowtype.STOR_TYPE_AZURE_BLOB:
		key = backend.BackendTypeAzure
	case flowtype.STOR_TYPE_CEPH_S3:
		key = backend.BackendTypeCeph
	case flowtype.STOR_TYPE_GCP_S3:
		key = backend.BackendTypeGcp
	default:
		log.Info("map tier to storage class name failed: backend type is not support.")
		return "", errors.New(DMERR_UnSupportBackendType)
	}

	className := ""
	log.Infof("key:%s\n", key)
	v1, _ := Int2ExtTierMap[key]
	v2, ok := (*v1)[tier]
	if !ok {
		err = fmt.Errorf("tier[%d] is not support for %s", tier, storageType)
	} else {
		className = v2
	}

	log.Infof("Storage class name of tier[%d] is %s.\n", tier, className)
	return className, err
}

func doInCloudTransition(acReq *datamover.LifecycleActionRequest) error {
	log.Infof("in-cloud transition action: transition %s from %d to %d of %s.\n",
		acReq.ObjKey, acReq.SourceTier, acReq.TargetTier, acReq.SourceBackend)

	loca, err := getBackendInfo(&acReq.SourceBackend, false)
	if err != nil {
		log.Infof("in-cloud transition of %s failed because get location failed.\n", acReq.ObjKey)
		return err
	}

	className, err := getStorageClassName(acReq.TargetTier, loca.StorType)
	if err != nil {
		log.Infof("in-cloud transition of %s failed because target tier is not supported.\n", acReq.ObjKey)
		return err
	}
	err = changeStorageClass(&acReq.ObjKey, &className, &acReq.BucketName, loca)
	if err != nil && err.Error() == DMERR_NoPermission {
		loca, err = getBackendInfo(&acReq.SourceBackend, true)
		if err != nil {
			return err
		}
		err = changeStorageClass(&acReq.ObjKey, &className, &acReq.BucketName, loca)
	}

	if err != nil {
		log.Infof("in-cloud transition of %s failed: %v.\n", acReq.ObjKey, err)
		return err
	}

	// update meta data.
	setting := make(map[string]string)
	setting[OBJMETA_TIER] = fmt.Sprintf("%d", acReq.TargetTier)
	req := osdss3.UpdateObjMetaRequest{ObjKey: acReq.ObjKey, BucketName: acReq.BucketName, Setting: setting, LastModified: acReq.LastModified}
	_, err = s3client.UpdateObjMeta(context.Background(), &req)
	if err != nil {
		// If update failed, it will be redo again in the next round of scheduling
		log.Infof("update tier of object[%s] to %d failed:%v.\n", acReq.ObjKey, acReq.TargetTier, err)
	}

	return err
}
