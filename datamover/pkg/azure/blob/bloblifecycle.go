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

package blobmover

import (
	"context"
	"errors"

	"github.com/Azure/azure-storage-blob-go/azblob"
	log "github.com/sirupsen/logrus"
	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
)

func (mover *BlobMover) setTier(objKey *string, newClass *string) error {
	ctx := context.Background()
	blobURL := mover.containerURL.NewBlockBlobURL(*objKey)
	var res *azblob.BlobSetTierResponse
	var err error
	switch *newClass {
	case string(azblob.AccessTierHot):
		res, err = blobURL.SetTier(ctx, azblob.AccessTierHot, azblob.LeaseAccessConditions{})
	case string(azblob.AccessTierCool):
		res, err = blobURL.SetTier(ctx, azblob.AccessTierCool, azblob.LeaseAccessConditions{})
	case string(azblob.AccessTierArchive):
		res, err = blobURL.SetTier(ctx, azblob.AccessTierArchive, azblob.LeaseAccessConditions{})
	default:
		log.Infof("[blobmover]set tier of object[%s] to %s failed, err: invalid storage class.\n", objKey, newClass)
		return errors.New("Invalid storage class")
	}
	if err != nil {
		log.Errorf("[blobmover]set tier of object[%s] to %s failed, err:%v\n", objKey, newClass, err)
	} else {
		log.Infof("[blobmover]set tier of object[%s] to %s succeed, res:%v\n", objKey, newClass, res.Response())
	}

	return err
}

func (mover *BlobMover) ChangeStorageClass(objKey *string, newClass *string, bkend *BackendInfo) error {
	log.Infof("")
	err := mover.Init(&bkend.EndPoint, &bkend.Access, &bkend.Security)
	if err != nil {
		return err
	}

	return mover.setTier(objKey, newClass)
}

func (mover *BlobMover) DeleteIncompleteMultipartUpload(objKey, uploadId string, destLoca *LocationInfo) error {
	log.Info("Azure blob does not support to delete incomplete multipart upload.")

	return errors.New(DMERR_InternalError)
}
