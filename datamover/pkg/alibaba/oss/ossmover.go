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
	"bytes"
	"errors"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
	pb "github.com/opensds/multi-cloud/datamover/proto"

	_ "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
	"io"
)

type OSSMover struct {
	ossClient          *oss.Client                        //for multipart upload and download
	multiUploadInitOut *oss.InitiateMultipartUploadResult //for multipart upload
	completeParts      []oss.UploadedPart
	ossBucket          *oss.Bucket //for multipart upload
}

var newstoreclasss oss.Option

func handleAlibabaOssErrors(err error) error {
	if err != nil {
		if serr, ok := err.(oss.ServiceError); ok { // This error is a Service-specific
			code := serr.Code
			switch code { // Compare serviceCode to ServiceCodeXxx constants
			case "SignatureDoesNotMatch":
				log.Info("hw-obs error: SignatureDoesNotMatch.")
				return errors.New(DMERR_NoPermission)
			case "NoSuchKey":
				log.Info("hw-obs error: NoSuchKey.")
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

//***************************************DownloadObj***************************************************//

func (mover *OSSMover) DownloadObj(objKey string, srcLoca *LocationInfo, buf []byte) (size int64, err error) {

	ossClient, err := oss.New(srcLoca.Access, srcLoca.Security, srcLoca.EndPoint)
	if err != nil {
		return 0, err
	}
	alibababucket, err := ossClient.Bucket(srcLoca.BucketName)
	log.Infof("[ossmover] Try to download, bucket:%s,obj:%s\n", srcLoca.BucketName, objKey)
	size = 0
	for tries := 1; tries <= 3; tries++ {

		result, err := alibababucket.GetObject(objKey)
		if err != nil {
			log.Errorf("[ossmover] download object[%s] failed %d times, err:%v", objKey, tries, err)
			e := handleAlibabaOssErrors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission || e.Error() == DMERR_NoSuchKey { //If no permission, then no need to retry.
				return 0, e
			}
		} else {
			size = 0
			defer result.Close()
			var readErr error
			var readCount int = 0
			// read object
			for {
				s := buf[size:]
				readCount, readErr = result.Read(s)
				if readCount > 0 {
					size += int64(readCount)
				}
				if readErr != nil {
					log.Errorf("[ossmover] readErr for object[%s] is:%v\n", objKey, readErr)
					break
				}
			}
			if readErr == io.EOF {
				readErr = nil
			}
			log.Infof("[ossmover] download object[%s] successfully.", objKey)
			return size, readErr
		}
	}

	log.Infof("[ossmover] download object[%s], should not be here.", objKey)
	return 0, errors.New(DMERR_InternalError)
}

//********************************UploadObj**************************************************//

func (mover *OSSMover) UploadObj(objKey string, destLoca *LocationInfo, buf []byte) error {
	log.Infof("[ossmover] try to upload object[%s], buf.len=%d.", objKey, len(buf))
	ossClient, err := oss.New(destLoca.Access, destLoca.Security, destLoca.EndPoint)
	if err != nil {
		log.Errorf("[ossmover] init obs failed for upload object[%s], err:%v.\n", objKey, err)
		return err
	}
	alibababucket, err := ossClient.Bucket(destLoca.BucketName)
	Body := bytes.NewReader(buf)

	if destLoca.ClassName != "" {
		newstoreclasss = StorageClassOSS(destLoca.ClassName)
	}
	for tries := 1; tries <= 3; tries++ {
		err = alibababucket.PutObject(objKey, Body, newstoreclasss)
		if err != nil {
			log.Errorf("[ossmover] put object[%s] failed %d times, err: %v\n", objKey, tries, err)
			e := handleAlibabaOssErrors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission {
				return e
			}
		} else {
			log.Infof("[ossmover] put object[%s] successfully", objKey)
			return nil
		}
	}

	log.Infof("[ossmover] Put object[%s], should not be here.\n", objKey)
	return errors.New(DMERR_InternalError)
}

//***********************************Delete*********************************************************//

func (mover *OSSMover) DeleteObj(objKey string, loca *LocationInfo) error {
	ossClient, err := oss.New(loca.Access, loca.Security, loca.EndPoint)
	if err != nil {
		log.Errorf("[ossmover] new client failed when delete obj[objKey:%s] in storage backend[type:oss], err:%v\n",
			objKey, err)
		return err
	}
	alibababucket, err := ossClient.Bucket(loca.BucketName)
	err = alibababucket.DeleteObject(objKey)
	log.Infof("[ossmover] try to Delete object[objKey:%s].", objKey)
	for tries := 1; tries <= 3; tries++ {

		if err != nil {
			log.Errorf("[ossmover] delete object[objKey:%s] in storage backend failed %d times, err:%v\n",
				objKey, tries, err)
			e := handleAlibabaOssErrors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission {
				return e
			}
		} else {
			log.Infof("[ossmover] delete object[objKey:%s] in storage backend successfully",
				objKey)
			return nil
		}
	}
	log.Infof("[ossmover] delete object[objKey:%s] in storage backend, should not be here.\n", objKey)
	return errors.New(DMERR_InternalError)
}

//*********************************************Multi-Part-Download-Init****************************************************//

func (mover *OSSMover) MultiPartDownloadInit(srcLoca *LocationInfo) error {
	var err error
	mover.ossClient, err = oss.New(srcLoca.Access, srcLoca.Security, srcLoca.EndPoint)
	if err != nil {
		log.Errorf("[ossmover] multiPart download init failed:%v\n", err)
	}
	return err
}

//********************************************DownloadRange*********************************************************//

func (mover *OSSMover) DownloadRange(objKey string, srcLoca *LocationInfo, buf []byte, start int64, end int64) (size int64, err error) {

	ossClient, err := oss.New(srcLoca.Access, srcLoca.Security, srcLoca.EndPoint)
	alibababucket, err := ossClient.Bucket(srcLoca.BucketName)
	log.Infof("[ossmover] try to download object[%s] range[%d - %d]...\n", objKey, start, end)
	for tries := 1; tries <= 3; tries++ {

		output, err := alibababucket.GetObject(objKey, oss.Range(start, end))

		if err != nil {
			log.Errorf("[ossmover] download object[%s] range[%d - %d] failed %d times.\n",
				objKey, start, end, tries)
			e := handleAlibabaOssErrors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission {
				return 0, e
			}
		} else {
			defer output.Close()
			readCount := 0
			var readErr error
			for {
				var rc int
				rc, readErr = output.Read(buf)
				if rc > 0 {
					readCount += rc
				}
				if readErr != nil {
					break
				}
			}
			if readErr != nil && readErr != io.EOF {
				log.Errorf("[ossmover] body.read for object[%s] failed, err:%v\n", objKey, err)
				return 0, readErr
			}
			log.Infof("[ossmover] download object[%s] range[%d - %d] successfully, readCount=%d.\n", objKey, start, end, readCount)
			return int64(readCount), nil
		}
	}

	log.Infof("[ossmover] download object[%s] range[%d - %d], should not be here.\n", objKey, start, end)
	return 0, errors.New(DMERR_InternalError)
}

//*************************************************Multi-Part-Init******************************************************//

func (mover *OSSMover) MultiPartUploadInit(objKey string, destLoca *LocationInfo) (string, error) {

	var err error
	mover.ossClient, err = oss.New(destLoca.Access, destLoca.Security, destLoca.EndPoint)
	mover.ossBucket, err = mover.ossClient.Bucket(destLoca.BucketName)

	if destLoca.ClassName != "" {
		newstoreclasss = StorageClassOSS(destLoca.ClassName)
	}

	if err != nil {
		log.Errorf("[ossmover] create obsclient failed in MultiPartUploadInit[obj:%s], err:%v\n", objKey, err)
		return "", err
	}
	log.Infof("[ossmover] try to InitiateMultipartUpload [objkey:%s].\n", objKey)
	for tries := 1; tries <= 3; tries++ {

		multipartresult, err := mover.ossBucket.InitiateMultipartUpload(objKey, newstoreclasss)
		mover.multiUploadInitOut = &multipartresult
		if err != nil {
			log.Errorf("[ossmover] init multipart upload [objkey:%s] failed %d times. err:%v\n", objKey, tries, err)
			e := handleAlibabaOssErrors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission {
				return "", e
			}
		} else {
			log.Infof("[ossmover] initiate multipart upload [objkey:%s] successfully.\n", objKey)
			return multipartresult.UploadID, nil
		}
	}

	log.Infof("[ossmover] initiate multipart upload [objkey:%s], should not be here.\n", objKey)
	return mover.multiUploadInitOut.UploadID, errors.New(DMERR_InternalError)
}

//**************************************Upload-part*******************************************//

func (mover *OSSMover) UploadPart(objKey string, destLoca *LocationInfo, upBytes int64, buf []byte, partNumber int64,
	offset int64) error {
	log.Infof("[ossmover] try to upload object[%s] range[partnumber#%d,offset#%d]...\n", objKey, partNumber, offset)

	input := oss.InitiateMultipartUploadResult{

		UploadID: mover.multiUploadInitOut.UploadID,
		Bucket:   destLoca.BucketName,
		Key:      objKey,
	}
	PartNumber := int(partNumber)
	Body := bytes.NewReader(buf)
	PartSize := upBytes

	for tries := 1; tries <= 3; tries++ {

		uploadresult, err := mover.ossBucket.UploadPart(input, Body, PartSize, PartNumber)
		if err != nil {
			log.Errorf("[ossmover] upload object[%s] range[partnumber#%d,offset#%d] failed %d times. err:%v\n",
				objKey, partNumber, offset, tries, err)
			e := handleAlibabaOssErrors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission {
				return e
			}
		} else {
			log.Infof("[ossmover] upload object[%s] range[partnumber#%d,offset#%d] successfully, size:%d\n",
				objKey, partNumber, offset, upBytes)
			mover.completeParts = append(mover.completeParts, oss.UploadedPart{
				ETag:       uploadresult.ETag,
				PartNumber: uploadresult.PartNumber})
			return nil
		}
	}

	log.Infof("[ossmover] upload object[%s] range[partnumber#%d,offset#%d], should not be here.\n",
		objKey, partNumber, offset)
	return errors.New(DMERR_InternalError)
}

//***********************************Complete-Multi-Part-Upload********************************************//

func (mover *OSSMover) CompleteMultipartUpload(objKey string, destLoca *LocationInfo) (err error) {

	input := oss.InitiateMultipartUploadResult{

		UploadID: mover.multiUploadInitOut.UploadID,
		Bucket:   destLoca.BucketName,
		Key:      objKey,
	}

	var completeParts []oss.UploadedPart
	for _, q := range mover.completeParts {

		CompletePart1 := oss.UploadedPart{
			ETag:       q.ETag,
			PartNumber: q.PartNumber,
		}

		completeParts = append(completeParts, CompletePart1)
	}

	log.Infof("[ossmover] try to CompleteMultipartUpload for object[%s].", objKey)
	for tries := 1; tries <= 3; tries++ {

		completemultipartresult, err := mover.ossBucket.CompleteMultipartUpload(input, completeParts)
		if err != nil {
			log.Errorf("[ossmover] complete multipart upload for object[%s] failed %d times, err:%v\n", objKey, tries, err)
			e := handleAlibabaOssErrors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission {
				return e
			}
		} else {
			log.Infof("[ossmover] complete multipart upload for object[%s] successfully,result:%v\n", objKey, completemultipartresult)
			return nil
		}
	}

	log.Infof("[ossmover] complete multipart upload for object[%s], should not be here\n", objKey)
	return errors.New(DMERR_InternalError)
}

//*********************************************Abort-Multipart-Upload****************************************//

func (mover *OSSMover) AbortMultipartUpload(objKey string, destLoca *LocationInfo) (err error) {

	input := oss.InitiateMultipartUploadResult{

		UploadID: mover.multiUploadInitOut.UploadID,
		Bucket:   destLoca.BucketName,
		Key:      objKey,
	}
	log.Infof("[ossmover] try to abort multipartupload, objkey:%s, uploadId:%s.\n",
		objKey, mover.multiUploadInitOut.UploadID)

	for tries := 1; tries <= 3; tries++ {

		err = mover.ossBucket.AbortMultipartUpload(input)
		if err != nil {
			log.Errorf("[ossmover] abort multipartupload failed %d times, objkey:%s, uploadId:%s, err:%v\n",
				tries, objKey, mover.multiUploadInitOut.UploadID, err)
			e := handleAlibabaOssErrors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission {
				return e
			}
		} else {
			log.Infof("[ossmover] abort multipartupload successfully, objkey:%s, uploadId:%s.\n",
				objKey, mover.multiUploadInitOut.UploadID)
			return nil
		}
	}
	log.Infof("[ossmover] abort multipartupload objkey:%s, uploadId:%s, should not be here.\n",
		objKey, mover.multiUploadInitOut.UploadID)
	return errors.New(DMERR_InternalError)
}

//*****************************************List-Objs************************************************************//

func ListObjs(loca *LocationInfo, filt *pb.Filter) ([]oss.ObjectProperties, error) {
	ossClient, err := oss.New(loca.Access, loca.Security, loca.EndPoint)

	alibababucket, err := ossClient.Bucket(loca.BucketName)
	marker := ""
	input, err := alibababucket.ListObjects(oss.Marker(marker))

	if filt != nil && filt.Prefix != "" {
		input.Prefix = filt.Prefix

	}

	if err != nil {
		log.Errorf("[ossmover] list objects failed, err:%v\n", err)
		return nil, handleAlibabaOssErrors(err)
	}
	objs := input.Objects
	for input.IsTruncated == true {
		input.Marker = input.NextMarker

		input, err := alibababucket.ListObjects(oss.Marker(marker))
		if err != nil {
			log.Errorf("[ossmover] list objects failed, err:%v\n", err)
			return nil, handleAlibabaOssErrors(err)
		}
		objs = append(objs, input.Objects...)
	}

	log.Infof("[ossmover] number of objects in bucket[%s] is %d.\n", loca.BucketName, len(objs))
	return input.Objects, nil
}
