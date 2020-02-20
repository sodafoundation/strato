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
package ossmover

import (
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
	log "github.com/sirupsen/logrus"
)

func (mover *OSSMover) ChangeStorageClass(objKey *string, newClass *string, bkend *BackendInfo) error {
	ossClient, err := oss.New(bkend.Access, bkend.Security, bkend.EndPoint)
	if err != nil {
		log.Errorf("[osslifecycle] new client failed when change storage class of obj[%s] to %s failed, err:%v\n",
			objKey, newClass, err)
		return err
	}
	alibababucket, err := ossClient.Bucket(bkend.BucketName)
	srcObj := objKey
	Obj := *srcObj
	storclass := newClass
	StorageClass := *storclass

	moverstorageresult, err := alibababucket.CopyObject(Obj, Obj, oss.ObjectStorageClass(oss.StorageClassType(StorageClass)))

	switch *newClass {
	case "STANDARD_IA":
		moverstorageresult.StorClass = oss.StorageIA
	case "GLACIER":
		moverstorageresult.StorClass = oss.StorageArchive
	default:
		log.Infof("[OSS] unspport storage class:%s", newClass)
		return handleAlibabaOssErrors(err)
	}
	if err != nil {
		log.Errorf("[OSS] change storage class failed: %v\n", err)
		return handleAlibabaOssErrors(err)
	} else {
		log.Infof("[OSS] change storage class succeed.\n", newClass)
	}
	return nil
}

func (mover *OSSMover) DeleteIncompleteMultipartUpload(objKey, uploadId string, loc *LocationInfo) error {
	log.Info("[osslifecycle] Abort multipart upload[objkey:%s] for uploadId#%s.\n", objKey, uploadId)

	ossclient, err := oss.New(loc.EndPoint, loc.Access, loc.Security)
	if err != nil {
		log.Info("[osslifecycle] New session failed, err:%v\n", err)
		return handleAlibabaOssErrors(err)
	}
	bucket, err := ossclient.Bucket(loc.BucketName)
	if err != nil {
		log.Info("[osslifecycle] getbucket failed, err:%v\n", err)
		return handleAlibabaOssErrors(err)
	}
	abortInput := oss.InitiateMultipartUploadResult{
		Bucket:   loc.BucketName,
		Key:      objKey,
		UploadID: uploadId,
	}
	err = bucket.AbortMultipartUpload(abortInput)
	e := handleAlibabaOssErrors(err)
	if e == nil || e.Error() == DMERR_NoSuchUpload {
		log.Info("[osslifecycle] abort multipart upload[objkey:%s, uploadid:%s] successfully.\n", objKey, uploadId)
		return nil
	} else {
		log.Info("[osslifecycle] abort multipart upload[objkey:%s, uploadid:%s] failed, err:%v.\n", objKey, uploadId, err)
	}

	return e
}

//************************************************************************//
func StorageClassOSS(ClassName string) oss.Option {
	var newclass oss.Option
	switch ClassName {
	case string(oss.StorageStandard):
		newclass = oss.StorageClass(oss.StorageStandard)
		return newclass
	case string(oss.StorageIA):
		newclass = oss.StorageClass(oss.StorageIA)
		return newclass
	case string(oss.StorageArchive):
		newclass = oss.StorageClass(oss.StorageArchive)
		return newclass
	}
	return nil

}
