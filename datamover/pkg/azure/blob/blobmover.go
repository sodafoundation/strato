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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"net/url"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	log "github.com/sirupsen/logrus"
	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
	pb "github.com/opensds/multi-cloud/datamover/proto"
)

var (
	HTTP_OK       = 200
	HTTP_CREATED  = 201
	HTTP_ACCEPTED = 202
)

//TryTimeout indicates the maximum time allowed for any single try of an HTTP request. 60 seconds per MB as default.
var MaxTimeForSingleHttpRequest = 16 * time.Minute

type BlobMover struct {
	containerURL  azblob.ContainerURL
	completeParts []string
}

func handleAzureBlobErrors(err error) error {
	if err != nil {
		if serr, ok := err.(azblob.StorageError); ok { // This error is a Service-specific
			code := string(serr.ServiceCode())
			switch code { // Compare serviceCode to ServiceCodeXxx constants
			case string(azblob.StorageErrorCodeAuthenticationFailed):
				log.Info("azure error: permission denied.")
				return errors.New(DMERR_NoPermission)
			default:
				return err
			}
		}
	}

	return nil
}

func (mover *BlobMover) Init(endpoint *string, acountName *string, accountKey *string) error {
	var err error
	mover.containerURL, err = mover.createContainerURL(endpoint, acountName, accountKey)
	if err != nil {
		log.Infof("[blobmover] init container URL faild:%v\n", err)
		return handleAzureBlobErrors(err)
	}

	log.Info("[blobmover] Init succeed, container URL:", mover.containerURL.String())
	return nil
}

func (mover *BlobMover) createContainerURL(endpoint *string, acountName *string, accountKey *string) (azblob.ContainerURL,
	error) {
	credential, err := azblob.NewSharedKeyCredential(*acountName, *accountKey)
	if err != nil {
		log.Infof("[blobmover] create credential failed, err:%v\n", err)
		return azblob.ContainerURL{}, handleAzureBlobErrors(err)
	}

	//create containerURL
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			TryTimeout: MaxTimeForSingleHttpRequest,
		},
	})
	URL, _ := url.Parse(*endpoint)

	return azblob.NewContainerURL(*URL, p), nil
}

func (mover *BlobMover) DownloadObj(objKey string, srcLoca *LocationInfo, buf []byte) (size int64, err error) {
	err = mover.Init(&srcLoca.EndPoint, &srcLoca.Access, &srcLoca.Security)
	if err != nil {
		return 0, handleAzureBlobErrors(err)
	}

	log.Infof("[blobmover] Try to download, bucket:%s,obj:%s\n", srcLoca.BucketName, objKey)
	ctx := context.Background()
	blobURL := mover.containerURL.NewBlockBlobURL(objKey)
	for tries := 1; tries <= 3; tries++ {
		downloadResp, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{},
			false)
		if err != nil {
			log.Infof("[blobmover] download object[%s] failed %d times, err:%v\n", objKey, tries, err)
			e := handleAzureBlobErrors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission { //If no permission, then no need to retry.
				return 0, e
			}
		} else {
			size = 0
			var readErr error
			var readCount int = 0
			for {
				s := buf[size:]
				readCount, readErr = downloadResp.Response().Body.Read(s)
				if readCount > 0 {
					size += int64(readCount)
				}
				if readErr != nil {
					log.Infof("[blobmover] readErr[objkey:%s]=%v\n", objKey, readErr)
					break
				}
			}
			if readErr == io.EOF {
				readErr = nil
			}
			log.Infof("[blobmover] Download object[%s] successfully.", objKey)
			return size, readErr
		}
	}

	log.Infof("[blobmover] download object[%s], should not be here.", objKey)
	return 0, errors.New(DMERR_InternalError)
}

func (mover *BlobMover) UploadObj(objKey string, destLoca *LocationInfo, buf []byte) error {
	err := mover.Init(&destLoca.EndPoint, &destLoca.Access, &destLoca.Security)
	if err != nil {
		return err
	}

	ctx := context.Background()
	blobURL := mover.containerURL.NewBlockBlobURL(objKey)
	log.Infof("[blobmover] Try to upload object[%s].", objKey)
	for tries := 1; tries <= 3; tries++ {
		uploadResp, err := blobURL.Upload(ctx, bytes.NewReader(buf), azblob.BlobHTTPHeaders{}, nil,
			azblob.BlobAccessConditions{})
		if err != nil {
			log.Infof("[blobmover] upload object[%s] failed %d times, err:%v\n", objKey, tries, err)
			e := handleAzureBlobErrors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission { //If no permission, then no need to retry.
				return e
			}
		} else if uploadResp.StatusCode() != HTTP_CREATED {
			log.Infof("[blobmover] upload object[%s] StatusCode:%d\n", objKey, uploadResp.StatusCode())
			if tries == 3 {
				return errors.New(DMERR_InternalError)
			}
		} else {
			log.Infof("[blobmover] Upload object[%s] successfully.", objKey)
			if destLoca.ClassName != "" {
				err := mover.setTier(&objKey, &destLoca.ClassName)
				if err != nil {
					// TODO:
				}
			}
			return nil
		}
	}

	log.Infof("[blobmover] upload object[%s], should not be here.", objKey)
	return errors.New(DMERR_InternalError)
}

func (mover *BlobMover) DeleteObj(objKey string, loca *LocationInfo) error {
	err := mover.Init(&loca.EndPoint, &loca.Access, &loca.Security)
	if err != nil {
		return err
	}

	ctx := context.Background()
	blobURL := mover.containerURL.NewBlockBlobURL(objKey)
	log.Infof("[blobmover] Try to delete object[%s].", objKey)
	for tries := 1; tries <= 3; tries++ {
		delRsp, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{})
		if err != nil {
			log.Infof("[blobmover] delete object[%s] failed %d times, err:%v\n", objKey, tries, err)
			e := handleAzureBlobErrors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission { //If no permission, then no need to retry.
				return e
			}
		} else if delRsp.StatusCode() == HTTP_OK || delRsp.StatusCode() == HTTP_ACCEPTED {
			log.Infof("[blobmover] delete object[%s] successfully.", objKey)
			return nil
		} else {
			log.Infof("[blobmover] delete object[%s] StatusCode:%d\n", objKey, delRsp.StatusCode())
			if tries >= 3 {
				return errors.New(DMERR_InternalError)
			}
		}
	}

	log.Infof("[blobmover] delete object[%s], should not be here.", objKey)
	return errors.New(DMERR_InternalError)
}

func (mover *BlobMover) MultiPartDownloadInit(srcLoca *LocationInfo) error {
	log.Infof("[blobmover] Prepare to do part upload, container:%s.\n", srcLoca.BucketName)

	return mover.Init(&srcLoca.EndPoint, &srcLoca.Access, &srcLoca.Security)
}

func (mover *BlobMover) DownloadRange(objKey string, srcLoca *LocationInfo, buf []byte, start int64, end int64) (size int64,
	err error) {
	log.Infof("[blobmover] Try to download object[%s] range[%d - %d]...\n", objKey, start, end)

	ctx := context.Background()
	blobURL := mover.containerURL.NewBlobURL(objKey)
	count := end - start + 1

	for tries := 1; tries <= 3; tries++ {
		err = azblob.DownloadBlobToBuffer(ctx, blobURL, start, count, buf, azblob.DownloadFromBlobOptions{})
		if err != nil {
			log.Infof("[blobomver] donwload object[%s] to buffer failed %d times, err:%v\n", objKey, tries, err)
			e := handleAzureBlobErrors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission { //If no permission, then no need to retry.
				return 0, e
			}
		} else {
			log.Infof("[blobmover] download object[%s] range[%d - %d] successfully.\n", objKey, start, end)
			return count, nil
		}
	}

	log.Infof("[blobmover] download object[%s] range[%d - %d], should not be here.\n", objKey, start, end)
	return 0, errors.New(DMERR_InternalError)
}

func (mover *BlobMover) MultiPartUploadInit(objKey string, destLoca *LocationInfo) (string, error) {
	log.Infof("[blobmover] Prepare to do part upload for object[%s], container:%s, blob:%s\n",
		objKey, destLoca.BucketName, objKey)

	return "", mover.Init(&destLoca.EndPoint, &destLoca.Access, &destLoca.Security)
}

func (mover *BlobMover) Int64ToBase64(blockID int64) string {
	buf := (&[8]byte{})[:]
	binary.LittleEndian.PutUint64(buf, uint64(blockID))
	return mover.BinaryToBase64(buf)
}

func (mover *BlobMover) BinaryToBase64(binaryID []byte) string {
	return base64.StdEncoding.EncodeToString(binaryID)
}

func (mover *BlobMover) Base64ToInt64(base64ID string) int64 {
	bin, _ := base64.StdEncoding.DecodeString(base64ID)
	return int64(binary.LittleEndian.Uint64(bin))
}

func (mover *BlobMover) UploadPart(objKey string, destLoca *LocationInfo, upBytes int64, buf []byte, partNumber int64,
	offset int64) error {
	log.Infof("[blobmover] Try to upload object[%s] range[partnumber#%d,offset#%d]...\n", objKey, partNumber, offset)
	//TODO: Consider that "A blob can have up to 100,000 uncommitted blocks, but their total size cannot exceed 200,000 MB."

	ctx := context.Background()
	blobURL := mover.containerURL.NewBlockBlobURL(objKey)
	base64ID := mover.Int64ToBase64(partNumber)
	for tries := 1; tries <= 3; tries++ {
		_, err := blobURL.StageBlock(ctx, base64ID, bytes.NewReader(buf), azblob.LeaseAccessConditions{}, nil)
		if err != nil {
			log.Infof("[blobmover] upload object[objkey:%s] part[%d] failed %d times. err:%v\n", objKey, partNumber, tries, err)
			e := handleAzureBlobErrors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission { //If no permission, then no need to retry.
				return e
			}
		} else {
			log.Infof("[blobmover] Upload range[objkey:%s, partnumber#%d, base64ID#%d] successfully.\n",
				objKey, partNumber, base64ID)
			mover.completeParts = append(mover.completeParts, base64ID)
			return nil
		}
	}

	log.Infof("[blobmover] upload range[objkey:%s, partnumber#%d, base64ID#%d], should not be here.\n",
		objKey, partNumber, base64ID)
	return errors.New(DMERR_InternalError)
}

func (mover *BlobMover) AbortMultipartUpload(objKey string, destLoca *LocationInfo) error {
	log.Infof("No need to abort multipart upload[objkey:%s].\n", objKey)
	return nil
}

//A blob can have up to 100,000 uncommitted blocks, but their total size cannot exceed 200,000 MB.
func (mover *BlobMover) CompleteMultipartUpload(objKey string, destLoca *LocationInfo) error {
	ctx := context.Background()
	blobURL := mover.containerURL.NewBlockBlobURL(objKey)

	log.Infof("[blobmover] Try to CompleteMultipartUpload of object[%s].\n", objKey)
	for tries := 1; tries <= 3; tries++ {
		_, err := blobURL.CommitBlockList(ctx, mover.completeParts, azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{})
		if err != nil {
			log.Infof("[blobmover] completeMultipartUpload of object[%s] failed:%v\n", objKey, err)
			e := handleAzureBlobErrors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission { //If no permission, then no need to retry.
				return e
			}
		} else {
			log.Infof("[blobmover] completeMultipartUpload of object[%s] successfully.\n", objKey)
			if destLoca.ClassName != "" {
				err := mover.setTier(&objKey, &destLoca.ClassName)
				if err != nil {
					mover.DeleteObj(objKey, destLoca)
					return err
				}
			}
			return nil
		}
	}

	log.Infof("[blobmover] completeMultipartUpload of object[%s], should not be here.\n", objKey)
	return errors.New(DMERR_InternalError)
}

func ListObjs(loca *LocationInfo, filt *pb.Filter) ([]azblob.BlobItem, error) {
	log.Infof("[blobmover] List objects of container[%s]\n", loca.BucketName)
	credential, err := azblob.NewSharedKeyCredential(loca.Access, loca.Security)
	if err != nil {
		log.Fatalf("[blobmover] create credential failed for list objects, err:%v\n", err)
		return nil, handleAzureBlobErrors(err)
	}

	//create containerURL
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	URL, _ := url.Parse(loca.EndPoint)
	containerURL := azblob.NewContainerURL(*URL, p)

	//TODO: Set the best context
	ctx := context.Background()

	var objs []azblob.BlobItem
	option := azblob.ListBlobsSegmentOptions{}
	if filt != nil {
		option.Prefix = filt.Prefix
	}
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := containerURL.ListBlobsFlatSegment(ctx, marker, option)
		if err != nil {
			log.Infof("[blobmover] listBlobsFlatSegment failed:%v\n", err)
			e := handleAzureBlobErrors(err)
			return nil, e
		}
		objs = append(objs, listBlob.Segment.BlobItems...)

		marker = listBlob.NextMarker
	}

	return objs, nil
}
