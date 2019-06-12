// Copyright (c) 2019 Click2Cloud Inc. All Rights Reserved.
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
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"io/ioutil"
	"strconv"

	"github.com/micro/go-log"
	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
	pb "github.com/opensds/multi-cloud/datamover/proto"
	. "github.com/webrtcn/s3client"
	"github.com/webrtcn/s3client/models"
)

type CreateMultipartUploadOutput struct {
	UploadID string
}

type GcpS3Mover struct {
	downloader         *Client                      //for multipart download
	svc                *Uploads                     //for multipart upload
	multiUploadInitOut *CreateMultipartUploadOutput //for multipart upload
	completeParts      []*CompletePart              //for multipart upload
}

func md5Content(data []byte) string {
	md5Ctx := md5.New()
	md5Ctx.Write(data)
	cipherStr := md5Ctx.Sum(nil)
	value := base64.StdEncoding.EncodeToString(cipherStr)
	return value
}

func (mover *GcpS3Mover) UploadObj(objKey string, destLoca *LocationInfo, buf []byte) error {
	log.Logf("[gcps3mover] upload object, key:%s.\n", objKey)
	sess := NewClient(destLoca.EndPoint, destLoca.Access, destLoca.Security)
	bucket := sess.NewBucket()
	gcpObject := bucket.NewObject(destLoca.BucketName)
	contentMD5 := md5Content(buf)
	length := int64(len(buf))
	body := ioutil.NopCloser(bytes.NewReader(buf))
	log.Logf("[gcps3mover] try to upload, bucket:%s,obj:%s\n", destLoca.BucketName, objKey)
	for tries := 1; tries <= 3; tries++ {
		err := gcpObject.Create(objKey, contentMD5, "", length, body, models.Private)
		if err != nil {
			log.Logf("[gcps3mover] upload object[%s] failed %d times, err:%v\n", objKey, tries, err)
			if tries == 3 {
				return err
			}
		} else {
			log.Logf("[gcps3mover] upload object[%s] successfully.\n", objKey)
			return nil
		}

	}
	log.Logf("[gcps3mover] upload object, bucket:%s,obj:%s, should not be here.\n", destLoca.BucketName, objKey)
	return errors.New(DMERR_InternalError)
}

func (mover *GcpS3Mover) DownloadObj(objKey string, srcLoca *LocationInfo, buf []byte) (size int64, err error) {
	log.Logf("[gcps3mover] DownloadObj object, key:%s.", objKey)
	sess := NewClient(srcLoca.EndPoint, srcLoca.Access, srcLoca.Security)
	bucket := sess.NewBucket()
	gcpObject := bucket.NewObject(srcLoca.BucketName)
	var numBytes int64
	log.Logf("[gcps3mover] Try to download, bucket:%s,obj:%s\n", srcLoca.BucketName, objKey)
	for tries := 1; tries <= 3; tries++ {
		getObject, err := gcpObject.Get(objKey, nil)
		//defer getObject.Body.Close()
		d, err := ioutil.ReadAll(getObject.Body)
		data := []byte(d)
		size = int64(len(data))
		copy(buf, data)

		if err != nil {
			log.Logf("[gcps3mover]download object[bucket:%s,key:%s] failed %d times, err:%v\n",
				srcLoca.BucketName, objKey, tries, err)
			if tries == 3 {
				return 0, err
			}
		} else {
			numBytes = getObject.ContentLength
			log.Logf("[gcps3mover]download object[bucket:%s,key:%s] succeed, bytes:%d\n", srcLoca.BucketName, objKey, numBytes)
			return numBytes, err
		}
	}

	log.Logf("[gcps3mover]download object[bucket:%s,key:%s], should not be here.\n", srcLoca.BucketName, objKey)
	return 0, errors.New(DMERR_InternalError)
}

func (mover *GcpS3Mover) MultiPartDownloadInit(srcLoca *LocationInfo) error {
	sess := NewClient(srcLoca.EndPoint, srcLoca.Access, srcLoca.Security)
	mover.downloader = sess
	log.Logf("[gcps3mover] MultiPartDownloadInit succeed.")

	return nil
}

func (mover *GcpS3Mover) DownloadRange(objKey string, srcLoca *LocationInfo, buf []byte, start int64, end int64) (size int64, err error) {
	log.Logf("[gcps3mover] Download object[%s] range[%d - %d]...\n", objKey, start, end)
	//sess := NewClient(srcLoca.EndPoint, srcLoca.Access, srcLoca.Security)
	bucket := mover.downloader.NewBucket()
	gcpObject := bucket.NewObject(srcLoca.BucketName)
	var getObjectOption GetObjectOption
	rangeObj := Range{Begin: start, End: end}
	getObjectOption = GetObjectOption{
		Range: &rangeObj,
	}
	strStart := strconv.FormatInt(start, 10)
	strEnd := strconv.FormatInt(end, 10)
	rg := "bytes=" + strStart + "-" + strEnd
	log.Logf("[gcps3mover] Try to download object:%s, range:=%s\n", objKey, rg)
	for tries := 1; tries <= 3; tries++ {
		resp, err := gcpObject.Get(objKey, &getObjectOption)
		//defer resp.Body.Close()
		d, err := ioutil.ReadAll(resp.Body)
		data := []byte(d)
		size = int64(len(data))
		copy(buf, data)
		if err != nil {
			log.Logf("[gcps3mover] download object[%s] range[%d - %d] failed %d times, err:%v\n",
				objKey, start, end, tries, err)
			if tries == 3 {
				return 0, err
			}
		} else {
			log.Logf("[gcps3mover] Download object[%s] range[%d - %d] succeed, bytes:%d\n", objKey, start, end, size)
			return size, err
		}
	}

	log.Logf("[gcps3mover] download object[%s] range[%d - %d], should not be here.\n", objKey, start, end)
	return 0, errors.New(DMERR_InternalError)
}

func (mover *GcpS3Mover) MultiPartUploadInit(objKey string, destLoca *LocationInfo) (string, error) {
	sess := NewClient(destLoca.EndPoint, destLoca.Access, destLoca.Security)
	bucket := sess.NewBucket()
	gcpObject := bucket.NewObject(destLoca.BucketName)
	mover.svc = gcpObject.NewUploads(objKey)
	log.Logf("[gcps3mover] Try to init multipart upload[objkey:%s].\n", objKey)
	for tries := 1; tries <= 3; tries++ {

		resp, err := mover.svc.Initiate(nil)
		if err != nil {
			log.Logf("[gcps3mover] init multipart upload[objkey:%s] failed %d times.\n", objKey, tries)
			if tries == 3 {
				return "", err
			}
		} else {
			mover.multiUploadInitOut = &CreateMultipartUploadOutput{resp.UploadID}
			log.Logf("[gcps3mover] init multipart upload[objkey:%s] successfully, UploadId:%s\n", objKey, resp.UploadID)
			return resp.UploadID, nil
		}
	}
	log.Logf("[gcps3mover] init multipart upload[objkey:%s], should not be here.\n", objKey)
	return "", errors.New(DMERR_InternalError)

}

func (mover *GcpS3Mover) UploadPart(objKey string, destLoca *LocationInfo, upBytes int64, buf []byte, partNumber int64, offset int64) error {
	log.Logf("[gcps3mover] Upload range[objkey:%s, partnumber#%d,offset#%d,upBytes#%d,uploadid#%s]...\n", objKey, partNumber,
		offset, upBytes, mover.multiUploadInitOut.UploadID)

	contentMD5 := md5Content(buf)
	length := int64(len(buf))
	data := []byte(buf)
	body := ioutil.NopCloser(bytes.NewReader(data))

	for tries := 1; tries <= 3; tries++ {
		upRes, err := mover.svc.UploadPart(int(partNumber), mover.multiUploadInitOut.UploadID, contentMD5, "", length, body)
		if err != nil {
			log.Logf("[gcps3mover] upload range[objkey:%s, partnumber#%d, offset#%d] failed %d times, err:%v\n",
				objKey, partNumber, offset, tries, err)
			if tries == 3 {
				return err
			}
		} else {

			mover.completeParts = append(mover.completeParts, upRes)
			log.Logf("[gcps3mover] Upload range[objkey:%s, partnumber#%d,offset#%d] successfully.\n", objKey, partNumber, offset)
			return nil
		}
	}
	log.Logf("[gcps3mover] upload range[objkey:%s, partnumber#%d, offset#%d], should not be here.\n", objKey, partNumber, offset)
	return errors.New(DMERR_InternalError)
}

func (mover *GcpS3Mover) AbortMultipartUpload(objKey string, destLoca *LocationInfo) error {
	log.Logf("[gcps3mover] Aborting multipart upload[objkey:%s] for uploadId#%s.\n", objKey, mover.multiUploadInitOut.UploadID)
	bucket := mover.downloader.NewBucket()
	gcpObject := bucket.NewObject(destLoca.BucketName)
	uploader := gcpObject.NewUploads(objKey)
	for tries := 1; tries <= 3; tries++ {
		err := uploader.RemoveUploads(mover.multiUploadInitOut.UploadID)
		if err != nil {
			log.Logf("[gcps3mover] abort multipart upload[objkey:%s] for uploadId#%s failed %d times.\n",
				objKey, mover.multiUploadInitOut.UploadID, tries)
			if tries == 3 {
				return err
			}
		} else {
			log.Logf("[gcps3mover] Abort multipart upload[objkey:%s] for uploadId#%s successfully.\n",
				objKey, mover.multiUploadInitOut.UploadID, tries)
			return nil
		}
	}
	log.Logf("[gcps3mover] abort multipart upload[objkey:%s] for uploadId#%s, should not be here.\n",
		objKey, mover.multiUploadInitOut.UploadID)
	return errors.New(DMERR_InternalError)
}

func (mover *GcpS3Mover) CompleteMultipartUpload(objKey string, destLoca *LocationInfo) error {
	log.Logf("[gcps3mover] Try to do CompleteMultipartUpload [objkey:%s].\n", objKey)
	var completeParts []CompletePart
	for _, p := range mover.completeParts {
		completePart := CompletePart{
			Etag:       p.Etag,
			PartNumber: int(p.PartNumber),
		}
		completeParts = append(completeParts, completePart)
	}
	for tries := 1; tries <= 3; tries++ {
		rsp, err := mover.svc.Complete(mover.multiUploadInitOut.UploadID, completeParts)
		if err != nil {
			log.Logf("[gcps3mover] completeMultipartUpload [objkey:%s] failed %d times, err:%v\n", objKey, tries, err)
			if tries == 3 {
				return err
			}
		} else {
			log.Logf("[gcps3mover] completeMultipartUpload successfully [objkey:%s], rsp:%v\n", objKey, rsp)
			return nil
		}
	}
	log.Logf("[gcps3mover] completeMultipartUpload [objkey:%s], should not be here.\n", objKey)
	return errors.New(DMERR_InternalError)
}

func (mover *GcpS3Mover) DeleteObj(objKey string, loca *LocationInfo) error {
	sess := NewClient(loca.EndPoint, loca.Access, loca.Security)
	bucket := sess.NewBucket()
	gcpObject := bucket.NewObject(loca.BucketName)

	err := gcpObject.Remove(objKey)

	if err != nil {
		log.Logf("[gcps3mover] error occurred while waiting for object[%s] to be deleted.\n", objKey)
		return err
	}

	log.Logf("[gcps3mover] Delete Object[%s] successfully.\n", objKey)
	return nil
}

func ListObjs(loca *LocationInfo, filt *pb.Filter) ([]models.GetBucketResponseContent, error) {

	sess := NewClient(loca.EndPoint, loca.Access, loca.Security)
	bucket := sess.NewBucket()
	var output *models.GetBucketResponse
	var err error
	if filt != nil {
		output, err = bucket.Get(string(loca.BucketName), filt.Prefix, "", "", 1000)

	} else {
		output, err = bucket.Get(string(loca.BucketName), "", "", "", 1000)
	}
	if err != nil {
		log.Logf("[gcps3mover] list bucket failed, err:%v\n", err)
		return nil, err
	}

	objs := output.Contents

	size := len(objs)

	var out []models.GetBucketResponseContent
	for i := 0; i < size; i++ {
		out = append(out, models.GetBucketResponseContent{
			Key:          objs[i].Key,
			Size:         objs[i].Size,
			StorageClass: objs[i].StorageClass,
			Owner:        objs[i].Owner,
			LastModified: objs[i].LastModified,
			Tag:          objs[i].Tag,
		})
	}
	log.Logf("[gcps3mover] Number of objects in bucket[%s] is %d.\n", loca.BucketName, len(objs))
	return output.Contents, nil
}
