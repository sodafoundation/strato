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

package obsmover

import (
	"errors"

	log "github.com/sirupsen/logrus"

	"github.com/opensds/multi-cloud/api/pkg/utils/obs"
	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
)

func (mover *ObsMover) ChangeStorageClass(objKey *string, newClass *string, bkend *BackendInfo) error {
	obsClient, err := obs.New(bkend.Access, bkend.Security, bkend.EndPoint)
	if err != nil {
		log.Errorf("[obslifecycle] new client failed when change storage class of obj[%s] to %s failed, err:%v\n",
			objKey, newClass, err)
		return err
	}

	input := &obs.CopyObjectInput{}
	input.Bucket = bkend.BucketName
	input.Key = *objKey
	input.CopySourceBucket = bkend.BucketName
	input.CopySourceKey = *objKey
	input.MetadataDirective = obs.CopyMetadata
	switch *newClass {
	case "STANDARD_IA":
		input.StorageClass = obs.StorageClassWarm
	case "GLACIER":
		input.StorageClass = obs.StorageClassCold
	default:
		log.Infof("[obslifecycle] unspport storage class:%s", newClass)
		return errors.New(DMERR_UnSupportStorageClass)
	}
	_, err = obsClient.CopyObject(input)
	if err != nil {
		log.Errorf("[obslifecycle] change storage class of object[%s] to %s failed: %v\n", objKey, newClass, err)
		e := handleHWObsErrors(err)
		return e
	}

	// TODO: How to make sure copy is complemented? Wait to see if the item got copied (example:svc.WaitUntilObjectExists)?

	return nil
}

func (mover *ObsMover) DeleteIncompleteMultipartUpload(objKey, uploadId string, loc *LocationInfo) error {
	obsClient, err := obs.New(loc.Access, loc.Security, loc.EndPoint)
	if err != nil {
		log.Errorf("[obslifecycle] new client failed when delete incomplete multipart upload[objkey=%s,uploadid=%s] failed, err:%v\n",
			objKey, uploadId, err)
		return err
	}

	input := &obs.AbortMultipartUploadInput{Bucket: loc.BucketName, Key: objKey, UploadId: uploadId}
	_, err = obsClient.AbortMultipartUpload(input)
	e := handleHWObsErrors(err)
	if e == nil || e.Error() == DMERR_NoSuchUpload {
		log.Infof("delete incomplete multipart upload[objkey=%s,uploadid=%s] successfully.\n", objKey, uploadId)
		return nil
	} else {
		log.Infof("delete incomplete multipart upload[objkey=%s,uploadid=%s] failed, err:%v.\n", objKey, uploadId, err)
	}

	return e
}
