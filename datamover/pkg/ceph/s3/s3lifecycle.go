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

package cephs3mover

import (
	"errors"
	log "github.com/sirupsen/logrus"
	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
	. "github.com/webrtcn/s3client"
)

func (mover *CephS3Mover) ChangeStorageClass(objKey *string, newClass *string, bkend *BackendInfo) error {
	log.Infof("[cephs3lifecycle]: Failed to change storage class of object[key:%s, backend:%s] to %s, no transition support for ceph s3.\n",
		objKey, bkend.BakendName, newClass)

	return errors.New(DMERR_UnSupportOperation)
}

func (mover *CephS3Mover) DeleteIncompleteMultipartUpload(objKey, uploadId string, loc *LocationInfo) error {
	log.Infof("[cephs3mover] Delete incomplete multipart upload[objkey=%s,uploadId=%s].\n", objKey, mover.multiUploadInitOut.UploadID)

	sess := NewClient(loc.EndPoint, loc.Access, loc.Security)
	bucket := sess.NewBucket()
	cephObject := bucket.NewObject(loc.BucketName)
	uploader := cephObject.NewUploads(objKey)
	err := uploader.RemoveUploads(uploadId)
	if err != nil {
		log.Errorf("[cephs3lifecycle] abort multipart upload[objkey:%s, uploadid:%s] failed, err:%v.\n", objKey, uploadId, err)
	} else {
		log.Infof("[cephs3lifecycle] abort multipart upload[objkey:%s, uploadid:%s] successfully, err:%v.\n", objKey, uploadId)
	}

	return err
}