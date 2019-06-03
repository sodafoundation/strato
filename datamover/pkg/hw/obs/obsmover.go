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
	"bytes"
	"errors"
	"io"

	"github.com/micro/go-log"
	"github.com/opensds/multi-cloud/api/pkg/utils/obs"
	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
	pb "github.com/opensds/multi-cloud/datamover/proto"
)

type ObsMover struct {
	obsClient          *obs.ObsClient                     //for multipart upload and download
	multiUploadInitOut *obs.InitiateMultipartUploadOutput //for multipart upload
	completeParts      []obs.Part                         //for multipart upload
}

func handleHWObsErrors(err error) error {
	if err != nil {
		if serr, ok := err.(obs.ObsError); ok { // This error is a Service-specific
			code := serr.Code
			switch code { // Compare serviceCode to ServiceCodeXxx constants
			case "SignatureDoesNotMatch":
				log.Log("hw-obs error: SignatureDoesNotMatch.")
				return errors.New(DMERR_NoPermission)
			case "NoSuchKey":
				log.Log("hw-obs error: NoSuchKey.")
				return errors.New(DMERR_NoSuchKey)
			case "NoSuchUpload":
				return errors.New(DMERR_NoSuchUpload)
			default:
				return err
			}
		}
	}

	return nil
}

func (mover *ObsMover) DownloadObj(objKey string, srcLoca *LocationInfo, buf []byte) (size int64, err error) {
	obsClient, err := obs.New(srcLoca.Access, srcLoca.Security, srcLoca.EndPoint)
	if err != nil {
		return 0, err
	}

	log.Logf("[obsmover] Try to download, bucket:%s,obj:%s\n", srcLoca.BucketName, objKey)
	input := &obs.GetObjectInput{}
	input.Bucket = srcLoca.BucketName
	input.Key = objKey
	size = 0
	for tries := 1; tries <= 3; tries++ {
		output, err := obsClient.GetObject(input)
		if err != nil {
			log.Logf("[obsmover] download object[%s] failed %d times, err:%v", objKey, tries, err)
			e := handleHWObsErrors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission || e.Error() == DMERR_NoSuchKey { //If no permission, then no need to retry.
				return 0, e
			}
		} else {
			size = 0
			defer output.Body.Close()
			log.Logf("[obsmover] get objcet[%s], StorageClass:%s, ETag:%s, ContentType:%s, ContentLength:%d, LastModified:%s\n",
				objKey, output.StorageClass, output.ETag, output.ContentType, output.ContentLength, output.LastModified)
			var readErr error
			var readCount int = 0
			// read object
			for {
				s := buf[size:]
				readCount, readErr = output.Body.Read(s)
				//log.Logf("readCount=%d, readErr=%v\n", readCount, readErr)
				if readCount > 0 {
					size += int64(readCount)
				}
				if readErr != nil {
					log.Logf("[obsmover] readErr for object[%s] is:%v\n", objKey, readErr)
					break
				}
			}
			if readErr == io.EOF {
				readErr = nil
			}
			log.Logf("[obsmover] download object[%s] successfully.", objKey)
			return size, readErr
		}
	}

	log.Logf("[obsmover] download object[%s], should not be here.", objKey)
	return 0, errors.New(DMERR_InternalError)
}

func (mover *ObsMover) UploadObj(objKey string, destLoca *LocationInfo, buf []byte) error {
	log.Logf("[obsmover] try to upload object[%s], buf.len=%d.", objKey, len(buf))
	obsClient, err := obs.New(destLoca.Access, destLoca.Security, destLoca.EndPoint)
	if err != nil {
		log.Logf("[obsmover] init obs failed for upload object[%s], err:%v.\n", objKey, err)
		return err
	}

	input := &obs.PutObjectInput{}
	input.Bucket = destLoca.BucketName
	input.Key = objKey
	input.Body = bytes.NewReader(buf)
	if destLoca.ClassName != "" {
		switch destLoca.ClassName {
		case string(obs.StorageClassStandard):
			input.StorageClass = obs.StorageClassStandard
		case string(obs.StorageClassWarm):
			input.StorageClass = obs.StorageClassWarm
		case string(obs.StorageClassCold):
			input.StorageClass = obs.StorageClassCold
		default:
			log.Logf("[obsmover] upload object[%s] failed, err: invalid storage class[%s].\n", objKey, destLoca.ClassName)
			return errors.New("invalid storage class")
		}
	}
	for tries := 1; tries <= 3; tries++ {
		output, err := obsClient.PutObject(input)
		if err != nil {
			log.Logf("[obsmover] put object[%s] failed %d times, err: %v\n", objKey, tries, err)
			e := handleHWObsErrors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission {
				return e
			}
		} else {
			log.Logf("[obsmover] put object[%s] successfully, RequestId:%s, ETag:%s\n",
				objKey, output.RequestId, output.ETag)
			return nil
		}
	}

	log.Logf("[obsmover] Put object[%s], should not be here.\n", objKey)
	return errors.New(DMERR_InternalError)
}

func (mover *ObsMover) DeleteObj(objKey string, loca *LocationInfo) error {
	obsClient, err := obs.New(loca.Access, loca.Security, loca.EndPoint)
	if err != nil {
		log.Logf("[obsmover] new client failed when delete obj[objKey:%s] in storage backend[type:hws], err:%v\n",
			objKey, err)
		return err
	}

	input := &obs.DeleteObjectInput{}
	input.Bucket = loca.BucketName
	input.Key = objKey
	log.Logf("[obsmover] try to Delete object[objKey:%s].", objKey)
	for tries := 1; tries <= 3; tries++ {
		output, err := obsClient.DeleteObject(input)
		if err != nil {
			log.Logf("[obsmover] delete object[objKey:%s] in storage backend failed %d times, err:%v\n",
				objKey, tries, err)
			e := handleHWObsErrors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission {
				return e
			}
		} else {
			log.Logf("[obsmover] delete object[objKey:%s] in storage backend successfully, RequestId:%s\n",
				objKey, output.RequestId)
			return nil
		}
	}
	log.Logf("[obsmover] delete object[objKey:%s] in storage backend, should not be here.\n", objKey)
	return errors.New(DMERR_InternalError)
}

func (mover *ObsMover) MultiPartDownloadInit(srcLoca *LocationInfo) error {
	var err error
	mover.obsClient, err = obs.New(srcLoca.Access, srcLoca.Security, srcLoca.EndPoint)
	if err != nil {
		log.Logf("[obsmover] multiPart download init failed:%v\n", err)
	}
	return err
}

func (mover *ObsMover) DownloadRange(objKey string, srcLoca *LocationInfo, buf []byte, start int64, end int64) (size int64, err error) {
	input := &obs.GetObjectInput{}
	input.Bucket = srcLoca.BucketName
	input.Key = objKey
	input.RangeStart = start
	input.RangeEnd = end
	log.Logf("[obsmover] try to download object[%s] range[%d - %d]...\n", objKey, start, end)
	for tries := 1; tries <= 3; tries++ {
		output, err := mover.obsClient.GetObject(input)
		if err != nil {
			log.Logf("[obsmover] download object[%s] range[%d - %d] failed %d times.\n",
				objKey, start, end, tries)
			e := handleHWObsErrors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission {
				return 0, e
			}
		} else {
			defer output.Body.Close()
			readCount := 0
			var readErr error
			for {
				var rc int
				rc, readErr = output.Body.Read(buf)
				if rc > 0 {
					readCount += rc
				}
				if readErr != nil {
					break
				}
			}
			if readErr != nil && readErr != io.EOF {
				log.Logf("[obsmover] body.read for object[%s] failed, err:%v\n", objKey, err)
				return 0, readErr
			}
			log.Logf("[obsmover] download object[%s] range[%d - %d] successfully, readCount=%d.\n", objKey, start, end, readCount)
			return int64(readCount), nil
		}
	}

	log.Logf("[obsmover] download object[%s] range[%d - %d], should not be here.\n", objKey, start, end)
	return 0, errors.New(DMERR_InternalError)
}

func (mover *ObsMover) MultiPartUploadInit(objKey string, destLoca *LocationInfo) (string, error) {
	input := &obs.InitiateMultipartUploadInput{}
	input.Bucket = destLoca.BucketName
	input.Key = objKey
	if destLoca.ClassName != "" {
		switch destLoca.ClassName {
		case string(obs.StorageClassStandard):
			input.StorageClass = obs.StorageClassStandard
		case string(obs.StorageClassWarm):
			input.StorageClass = obs.StorageClassWarm
		case string(obs.StorageClassCold):
			input.StorageClass = obs.StorageClassCold
		default:
			log.Logf("[obsmover] upload object[%s] failed, err: invalid storage class[%s].\n", objKey, destLoca.ClassName)
			return "", errors.New("invalid storage class")
		}
	}
	var err error = nil
	mover.obsClient, err = obs.New(destLoca.Access, destLoca.Security, destLoca.EndPoint)
	if err != nil {
		log.Logf("[obsmover] create obsclient failed in MultiPartUploadInit[obj:%s], err:%v\n", objKey, err)
		return "", err
	}
	log.Logf("[obsmover] try to InitiateMultipartUpload [objkey:%s].\n", objKey)
	for tries := 1; tries <= 3; tries++ {
		mover.multiUploadInitOut, err = mover.obsClient.InitiateMultipartUpload(input)
		if err != nil {
			log.Logf("[obsmover] init multipart upload [objkey:%s] failed %d times. err:%v\n", objKey, tries, err)
			e := handleHWObsErrors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission {
				return "", e
			}
		} else {
			log.Logf("[obsmover] initiate multipart upload [objkey:%s] successfully.\n", objKey)
			return mover.multiUploadInitOut.UploadId, nil
		}
	}

	log.Logf("[obsmover] initiate multipart upload [objkey:%s], should not be here.\n", objKey)
	return "", errors.New(DMERR_InternalError)
}

func (mover *ObsMover) UploadPart(objKey string, destLoca *LocationInfo, upBytes int64, buf []byte, partNumber int64,
	offset int64) error {
	log.Logf("[obsmover] try to upload object[%s] range[partnumber#%d,offset#%d]...\n", objKey, partNumber, offset)
	uploadPartInput := &obs.UploadPartInput{}
	uploadPartInput.Bucket = destLoca.BucketName
	uploadPartInput.Key = objKey
	uploadPartInput.UploadId = mover.multiUploadInitOut.UploadId
	uploadPartInput.Body = bytes.NewReader(buf)
	uploadPartInput.PartNumber = int(partNumber)
	uploadPartInput.Offset = offset
	uploadPartInput.PartSize = upBytes
	for tries := 1; tries <= 3; tries++ {
		uploadPartInput.Body = bytes.NewReader(buf)
		uploadPartInputOutput, err := mover.obsClient.UploadPart(uploadPartInput)
		if err != nil {
			log.Logf("[obsmover] upload object[%s] range[partnumber#%d,offset#%d] failed %d times. err:%v\n",
				objKey, partNumber, offset, tries, err)
			e := handleHWObsErrors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission {
				return e
			}
		} else {
			log.Logf("[obsmover] upload object[%s] range[partnumber#%d,offset#%d] successfully, size:%d\n",
				objKey, partNumber, offset, upBytes)
			mover.completeParts = append(mover.completeParts, obs.Part{
				ETag:       uploadPartInputOutput.ETag,
				PartNumber: uploadPartInputOutput.PartNumber})
			return nil
		}
	}

	log.Logf("[obsmover] upload object[%s] range[partnumber#%d,offset#%d], should not be here.\n",
		objKey, partNumber, offset)
	return errors.New(DMERR_InternalError)
}

func (mover *ObsMover) AbortMultipartUpload(objKey string, destLoca *LocationInfo) (err error) {
	input := &obs.AbortMultipartUploadInput{}
	input.Bucket = destLoca.BucketName
	input.Key = objKey
	input.UploadId = mover.multiUploadInitOut.UploadId
	log.Logf("[obsmover] try to abort multipartupload, objkey:%s, uploadId:%s.\n",
		objKey, mover.multiUploadInitOut.UploadId)
	for tries := 1; tries <= 3; tries++ {
		_, err = mover.obsClient.AbortMultipartUpload(input)
		if err != nil {
			log.Logf("[obsmover] abort multipartupload failed %d times, objkey:%s, uploadId:%s, err:%v\n",
				tries, objKey, mover.multiUploadInitOut.UploadId, err)
			e := handleHWObsErrors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission {
				return e
			}
		} else {
			log.Logf("[obsmover] abort multipartupload successfully, objkey:%s, uploadId:%s.\n",
				objKey, mover.multiUploadInitOut.UploadId)
			return nil
		}
	}
	log.Logf("[obsmover] abort multipartupload objkey:%s, uploadId:%s, should not be here.\n",
		objKey, mover.multiUploadInitOut.UploadId)
	return errors.New(DMERR_InternalError)
}

func (mover *ObsMover) CompleteMultipartUpload(objKey string, destLoca *LocationInfo) (err error) {
	completeMultipartUploadInput := &obs.CompleteMultipartUploadInput{}
	completeMultipartUploadInput.Bucket = destLoca.BucketName
	completeMultipartUploadInput.Key = objKey
	completeMultipartUploadInput.UploadId = mover.multiUploadInitOut.UploadId
	completeMultipartUploadInput.Parts = mover.completeParts
	log.Logf("[obsmover] try to CompleteMultipartUpload for object[%s].", objKey)
	for tries := 1; tries <= 3; tries++ {
		_, err = mover.obsClient.CompleteMultipartUpload(completeMultipartUploadInput)
		if err != nil {
			log.Logf("[obsmover] complete multipart upload for object[%s] failed %d times, err:%v\n", objKey, tries, err)
			e := handleHWObsErrors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission {
				return e
			}
		} else {
			log.Logf("[obsmover] complete multipart upload for object[%s] successfully\n", objKey)
			return nil
		}
	}

	log.Logf("[obsmover] complete multipart upload for object[%s], should not be here\n", objKey)
	return errors.New(DMERR_InternalError)
}

//TODO: Need to support list object page by page
func ListObjs(loca *LocationInfo, filt *pb.Filter) ([]obs.Content, error) {
	obsClient, _ := obs.New(loca.Access, loca.Security, loca.EndPoint)
	input := &obs.ListObjectsInput{
		Bucket: loca.BucketName,
		Marker: "0",
	}
	if filt != nil && filt.Prefix != "" {
		input.ListObjsInput.Prefix = filt.Prefix
	}
	output, err := obsClient.ListObjects(input)
	if err != nil {
		log.Logf("[obsmover] list objects failed, err:%v\n", err)
		return nil, handleHWObsErrors(err)
	}
	objs := output.Contents
	for output.IsTruncated == true {
		input.Marker = output.NextMarker
		output, err = obsClient.ListObjects(input)
		if err != nil {
			log.Logf("[obsmover] list objects failed, err:%v\n", err)
			return nil, handleHWObsErrors(err)
		}
		objs = append(objs, output.Contents...)
	}

	log.Logf("[obsmover] number of objects in bucket[%s] is %d.\n", loca.BucketName, len(objs))
	return objs, nil
}
