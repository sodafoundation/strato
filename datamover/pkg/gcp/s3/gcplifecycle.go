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

package Gcps3mover

import (
	"errors"

	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
	log "github.com/sirupsen/logrus"
)

func (mover *GcpS3Mover) ChangeStorageClass(objKey *string, newClass *string, bkend *BackendInfo) error {
	log.Info("gcp s3 change storage class of %s to %s failed.", objKey, newClass)
	return errors.New(DMERR_InternalError)
}

func (mover *GcpS3Mover) DeleteIncompleteMultipartUpload(objKey, uploadId string, destLoca *LocationInfo) error {
	log.Info("gcp s3 does not support to delete incomplete multipart upload.")

	return errors.New(DMERR_InternalError)
}
