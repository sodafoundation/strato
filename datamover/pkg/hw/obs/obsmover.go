// Copyright (c) 2018 Huawei Technologies Co., Ltd. All Rights Reserved.
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
	"io"
	"obs"
	"github.com/micro/go-log"
	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
	pb "github.com/opensds/multi-cloud/datamover/proto"
)

type ObsMover struct{
	obsClient *obs.ObsClient //for multipart upload and download
	multiUploadInitOut *obs.InitiateMultipartUploadOutput //for multipart upload
	completeParts []obs.Part //for multipart upload
}

func (mover *ObsMover)DownloadObj(objKey string, srcLoca *LocationInfo, buf []byte) (size int64, err error) {
	obsClient, err := obs.New(srcLoca.Access, srcLoca.Security, srcLoca.EndPoint)
	if err != nil {
		return 0, err
	}

	log.Logf("[obsmover] Try to download, bucket:%s,obj:%s\n", srcLoca.BucketName, objKey)
	input := &obs.GetObjectInput{}
	input.Bucket = srcLoca.BucketName
	input.Key = objKey
	output, err := obsClient.GetObject(input)
	if err == nil {
		size = 0
		defer output.Body.Close()
		log.Logf("[obsmover] Get objcet[%s], StorageClass:%s, ETag:%s, ContentType:%s, ContentLength:%d, LastModified:%s\n",
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
		log.Logf("[obsmover] Download object[%s] successfully.", objKey)
		return size, readErr
	} else if obsError, ok := err.(obs.ObsError); ok {
		log.Logf("[obsmover] Download object[%s] failed, code:%s,message:%s", objKey, obsError.Code, obsError.Message)
		return 0, err
	}
	return
}

func (mover *ObsMover)UploadObj(objKey string, destLoca *LocationInfo, buf []byte) error {
	log.Logf("[obsmover] Upload object[%s], buf.len=%d.", objKey, len(buf))
	obsClient, err := obs.New(destLoca.Access, destLoca.Security, destLoca.EndPoint)
	if err != nil {
		log.Logf("[obsmover] Init obs failed for upload object[%s], err:%v.\n", objKey, err)
		return err
	}

	input := &obs.PutObjectInput{}
	input.Bucket = destLoca.BucketName
	input.Key = objKey
	input.Body = bytes.NewReader(buf)
	output, err := obsClient.PutObject(input)
	if err != nil {
		log.Logf("[obsmover] Put object[%s] to obs failed, err: %v\n", objKey, err)
	} else {
		log.Logf("[obsmover] Put object[%s] to obs successfully, RequestId:%s, ETag:%s\n",
			objKey, output.RequestId, output.ETag)
	}

	return err
}

func (mover *ObsMover)DeleteObj(objKey string, loca *LocationInfo) error {
	obsClient, err := obs.New(loca.Access, loca.Security, loca.EndPoint)
	if err != nil {
		log.Logf("[obsmover] New client failed when delete obj[objKey:%s] in storage backend[type:hws], err:%v\n",
			objKey, err)
		return err
	}

	input := &obs.DeleteObjectInput{}
	input.Bucket = loca.BucketName
	input.Key = objKey

	output, err := obsClient.DeleteObject(input)
	if err == nil {
		log.Logf("[obsmover] Delete object[objKey:%s] in storage backend successfully, RequestId:%s\n",
			objKey, output.RequestId)
	} else {
		log.Logf("[obsmover] Delete object[objKey:%s] in storage backend failed, err:%v\n", objKey, err)
	}

	return err
}

func (mover *ObsMover)MultiPartDownloadInit(srcLoca *LocationInfo) error {
	var err error
	mover.obsClient, err = obs.New(srcLoca.Access, srcLoca.Security, srcLoca.EndPoint)
	if err != nil {
		log.Logf("[obsmover] MultiPartDownloadInit failed:%v\n", err)
	}
	return err
}

func (mover *ObsMover)DownloadRange(objKey string, srcLoca *LocationInfo, buf []byte, start int64, end int64) (size int64, err error) {
	log.Logf("[obsmover] Download object[%s] range[%d - %d]...\n", objKey, start, end)
	input := &obs.GetObjectInput{}
	input.Bucket = srcLoca.BucketName
	input.Key = objKey
	input.RangeStart = start
	input.RangeEnd = end
	log.Logf("[obsmover] Try to download object[%s] start:%d, end:%d\n", objKey, start, end)

	//obsClient,_ := obs.New(srcLoca.Access, srcLoca.Security, srcLoca.EndPoint)
	output, err := mover.obsClient.GetObject(input)
	if err != nil {
		log.Logf("[obsmover] GetObject[%s] failed, err:%v\n", objKey, err)
		return 0,err
	}
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
	if readErr != nil && readErr != io.EOF{
		log.Logf("[obsmover] Body.read for object[%s] failed, err:%v\n", objKey, err)
		return 0,readErr
	}

	//log.Logf("Download readCount=%d\nbuf:%s\n", readCount,buf[:readCount])
	log.Logf("[obsmover] Download object[%s] range[%d - %d] successfully, readCount=%d.\n", objKey, start, end, readCount)
	return int64(readCount), nil
}

func (mover *ObsMover)MultiPartUploadInit(objKey string, destLoca *LocationInfo) error{
	input := &obs.InitiateMultipartUploadInput{}
	input.Bucket = destLoca.BucketName
	input.Key = objKey
	var err error = nil
	mover.obsClient,err = obs.New(destLoca.Access, destLoca.Security, destLoca.EndPoint)
	if err != nil {
		log.Logf("[obsmover] Create obsclient failed in MultiPartUploadInit[obj:%s], err:%v\n", objKey, err)
		return err
	}
	mover.multiUploadInitOut,err = mover.obsClient.InitiateMultipartUpload(input)
	if err != nil {
		log.Logf("[obsmover] InitiateMultipartUpload[obj:%s] failed, err:%v\n", objKey, err)
		return err
	}

	return nil
}

func (mover *ObsMover)UploadPart(objKey string, destLoca *LocationInfo, upBytes int64, buf []byte, partNumber int64,
	offset int64) error {
	log.Logf("[obsmover] Upload object[%s] range[partnumber#%d,offset#%d]...\n", objKey, partNumber, offset)
	uploadPartInput := &obs.UploadPartInput{}
	uploadPartInput.Bucket = destLoca.BucketName
	uploadPartInput.Key = objKey
	uploadPartInput.UploadId = mover.multiUploadInitOut.UploadId
	uploadPartInput.Body = bytes.NewReader(buf)
	uploadPartInput.PartNumber = int(partNumber)
	uploadPartInput.Offset = offset
	uploadPartInput.PartSize = upBytes
	tries := 1
	for tries <= 3 {
		uploadPartInputOutput, err := mover.obsClient.UploadPart(uploadPartInput)
		if err != nil {
			if tries == 3 {
				log.Logf("[obsmover] Upload object[%s] range[partnumber#%d,offset#%d] failed. err:%v\n",
					objKey, partNumber, offset, err)
				return err
			}
			log.Logf("[obsmover] Retrying to upload object[%s] part#%d\n", objKey, partNumber)
			tries++
		}else {
			log.Logf("[obsmover] Upload object[%s] range[partnumber#%d,offset#%d] successfully, size:%d\n",
				objKey, partNumber, offset, upBytes)
			mover.completeParts = append(mover.completeParts, obs.Part{
				ETag: uploadPartInputOutput.ETag,
				PartNumber: uploadPartInputOutput.PartNumber})
			break
		}
	}

	return nil
}

func (mover *ObsMover)AbortMultipartUpload(objKey string, destLoca *LocationInfo) error {
	input := &obs.AbortMultipartUploadInput{}
	input.Bucket = destLoca.BucketName
	input.Key = objKey
	input.UploadId = mover.multiUploadInitOut.UploadId

	_, err := mover.obsClient.AbortMultipartUpload(input)
	log.Logf("[obsmover] Abort multipartupload finish, objkey:%s, uploadId:%s, err:%v\n",
		objKey, mover.multiUploadInitOut.UploadId, err)

	return err
}

func (mover *ObsMover)CompleteMultipartUpload(objKey string, destLoca *LocationInfo) error {
	completeMultipartUploadInput := &obs.CompleteMultipartUploadInput{}
	completeMultipartUploadInput.Bucket = destLoca.BucketName
	completeMultipartUploadInput.Key = objKey
	completeMultipartUploadInput.UploadId = mover.multiUploadInitOut.UploadId
	completeMultipartUploadInput.Parts = mover.completeParts
	_, err := mover.obsClient.CompleteMultipartUpload(completeMultipartUploadInput)
	if err != nil {
		//panic(err)
		log.Logf("[obsmover] CompleteMultipartUpload for object[%s] failed, err:%v\n", objKey, err)
	}else {
		log.Logf("[obsmover] CompleteMultipartUpload for object[%s] successfully", objKey)
	}

	return err
}

func ListObjs(loca *LocationInfo, filt *pb.Filter) ([]obs.Content, error) {
	obsClient,_ := obs.New(loca.Access, loca.Security, loca.EndPoint)
	input := &obs.ListObjectsInput{
		Bucket: loca.BucketName,
		Marker: "0",
	}
	if filt != nil && filt.Prefix != "" {
		input.ListObjsInput.Prefix = filt.Prefix
	}
	output, err := obsClient.ListObjects(input)
	if err != nil {
		log.Logf("[obsmover] List objects failed, err:%v\n", err)
		return nil,err
	}
	objs := output.Contents
	for ; output.IsTruncated == true ; {
		input.Marker = output.NextMarker
		output, err = obsClient.ListObjects(input)
		if err != nil {
			log.Logf("[obsmover] List objects failed, err:%v\n", err)
			return nil,err
		}
		objs = append(objs, output.Contents...)
	}

	log.Logf("[obsmover] Number of objects in bucket[%s] is %d.\n", loca.BucketName, len(objs))
	return objs,nil
}