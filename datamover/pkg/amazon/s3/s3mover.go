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

package s3mover

import (
	"bytes"
	"errors"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/micro/go-log"
	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
)

type S3Mover struct{
	svc *s3.S3 //for multipart upload
	multiUploadInitOut *s3.CreateMultipartUploadOutput //for multipart upload
	//uploadId string //for multipart upload
	completeParts []*s3.CompletedPart //for multipart upload
}

type s3Cred struct {
	ak string
	sk string
}

func (myc *s3Cred) Retrieve() (credentials.Value, error) {
	cred := credentials.Value{AccessKeyID: myc.ak, SecretAccessKey: myc.sk}
	return cred, nil
}

func (myc *s3Cred) IsExpired() bool {
	return false
}

func (mover *S3Mover)UploadObj(objKey string, destLoca *LocationInfo, buf []byte) error {
	log.Log("UploadObj of s3mover is called.")
	log.Logf("destLoca.Region=%s, destLoca.EndPoint=%s\n", destLoca.Region, destLoca.EndPoint)
	s3c := s3Cred{ak: destLoca.Access, sk: destLoca.Security}
	creds := credentials.NewCredentials(&s3c)
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(destLoca.Region),
		Endpoint:    aws.String(destLoca.EndPoint),
		Credentials: creds,
	})
	if err != nil {
		log.Logf("New session failed, err:%v\n", err)
		return err
	}

	reader := bytes.NewReader(buf)
	uploader := s3manager.NewUploader(sess)
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(destLoca.BucketName),
		Key:    aws.String(objKey),
		Body:   reader,
	})
	if err != nil {
		log.Logf("Upload object to aws failed, err:%v\n", err)
	} else {
		log.Log("Upload object to aws succeed.")
	}

	return err
}

func (mover *S3Mover)DownloadObj(objKey string, srcLoca *LocationInfo, buf []byte) (size int64, err error) {
	s3c := s3Cred{ak: srcLoca.Access, sk: srcLoca.Security}
	creds := credentials.NewCredentials(&s3c)
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(srcLoca.Region),
		Endpoint:    aws.String(srcLoca.EndPoint),
		Credentials: creds,
	})
	if err != nil {
		log.Logf("New session failed, err:%v\n", err)
		return 0, err
	}

	writer := aws.NewWriteAtBuffer(buf)
	downLoader := s3manager.NewDownloader(sess)
	getObjInput := s3.GetObjectInput{
		Bucket:aws.String(srcLoca.BucketName),
		Key:aws.String(objKey),
	}
	log.Logf("Try to download, bucket:%s,obj:%s\n", srcLoca.BucketName, objKey)
	numBytes, err := downLoader.Download(writer, &getObjInput)
	if err != nil {
		log.Logf("download object[bucket:%s,key:%s] failed, err:%v\n", srcLoca.BucketName,objKey,err)
	} else {
		log.Logf("downlad object[bucket:%s,key:%s] succeed, bytes:%d\n", srcLoca.BucketName,objKey,numBytes)
	}

	return numBytes, err
}

func (mover *S3Mover)DownloadRange(objKey string, srcLoca *LocationInfo, buf []byte, start int64, end int64) (size int64, err error) {
	s3c := s3Cred{ak: srcLoca.Access, sk: srcLoca.Security}
	creds := credentials.NewCredentials(&s3c)
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(srcLoca.Region),
		Endpoint:    aws.String(srcLoca.EndPoint),
		Credentials: creds,
	})
	if err != nil {
		log.Logf("New session failed, err:%v\n", err)
		return 0, err
	}

	writer := aws.NewWriteAtBuffer(buf)
	downLoader := s3manager.NewDownloader(sess)
	getObjInput := s3.GetObjectInput{
		Bucket: aws.String(srcLoca.BucketName),
		Key:    aws.String(objKey),
	}
	strStart := strconv.FormatInt(start, 10)
	strEnd := strconv.FormatInt(end, 10)
	rg := "bytes=" + strStart + "-" + strEnd
	getObjInput.SetRange(rg)
	log.Logf("range:=%s\n", rg)
	numBytes, err := downLoader.Download(writer, &getObjInput)

	if err != nil {
		log.Logf("download faild, err:%v\n", err)
	} else {
		log.Logf("downlad succeed, bytes:%d\n", numBytes)
	}

	return numBytes, err
}

func (mover *S3Mover)MultiPartUploadInit(objKey string, destLoca *LocationInfo) error {
	s3c := s3Cred{ak: destLoca.Access, sk: destLoca.Security}
	creds := credentials.NewCredentials(&s3c)
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(destLoca.Region),
		Endpoint:    aws.String(destLoca.EndPoint),
		Credentials: creds,
	})
	if err != nil {
		log.Logf("New session failed, err:%v\n", err)
		return err
	}

	mover.svc = s3.New(sess)
	multiUpInput := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(destLoca.BucketName),
		Key:    aws.String(objKey),
	}
	resp, err := mover.svc.CreateMultipartUpload(multiUpInput)
	if err != nil {
		log.Logf("Init s3 multipart upload failed, err:%v\n", err)
		return errors.New("Init s3 multipart upload failed.")
	} else {
		log.Logf("Init s3 multipart upload succeed, UploadId:%s\n", resp.UploadId)
	}

	//mover.uploadId = *resp.UploadId
	mover.multiUploadInitOut = resp
	return nil
}

func (mover *S3Mover)UploadPart(objKey string, destLoca *LocationInfo, upBytes int64, buf []byte, partNumber int64, offset int64) error {
	tries := 1
	upPartInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(buf),
		Bucket:        aws.String(destLoca.BucketName),
		Key:           aws.String(objKey),
		PartNumber:    aws.Int64(partNumber),
		UploadId:      aws.String(*mover.multiUploadInitOut.UploadId),
		ContentLength: aws.Int64(upBytes),
	}

	log.Logf("len(buf)=%d, upBytes=%d,uploadId=%s,partNumber=%d\n", len(buf), upBytes, *mover.multiUploadInitOut.UploadId, partNumber)

	for tries <= 3 {
		upRes, err := mover.svc.UploadPart(upPartInput)
		if err != nil {
			if tries == 3 {
				log.Logf("Upload part to aws failed. err:%v\n", err)
				return err
			}
			log.Logf("Retrying to upload part#%d\n", partNumber)
			tries++
		} else {
			log.Logf("Uploaded part #%d\n", partNumber)
			part := s3.CompletedPart{
				ETag:       upRes.ETag,
				PartNumber: aws.Int64(partNumber),
			}
			mover.completeParts = append(mover.completeParts, &part)
			break
		}
	}

	return nil
}

func (mover *S3Mover)AbortMultipartUpload(objKey string, destLoca *LocationInfo) error {
	log.Logf("Aborting multipart upload for uploadId#%s.\n", *mover.multiUploadInitOut.UploadId)
	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(destLoca.BucketName),
		Key:      aws.String(objKey),
		UploadId: aws.String(*mover.multiUploadInitOut.UploadId),
	}

	_, err := mover.svc.AbortMultipartUpload(abortInput)
	return err
}

func (mover *S3Mover)CompleteMultipartUpload(objKey string, destLoca *LocationInfo) error {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(destLoca.BucketName),
		Key:      aws.String(objKey),
		UploadId: aws.String(*mover.multiUploadInitOut.UploadId),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: mover.completeParts,
		},
	}

	rsp, err := mover.svc.CompleteMultipartUpload(completeInput)
	if err != nil {
		log.Logf("completeMultipartUploadS3 failed, err:%v\n", err)
	} else {
		log.Logf("completeMultipartUploadS3 successfully, rsp:%v\n", rsp)
	}

	return err
}

func (mover *S3Mover)DeleteObj(objKey string, loca *LocationInfo) error {
	s3c := s3Cred{ak: loca.Access, sk: loca.Security}
	creds := credentials.NewCredentials(&s3c)
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(loca.Region),
		Endpoint:    aws.String(loca.EndPoint),
		Credentials: creds,
	})
	if err != nil {
		log.Logf("New session failed, err:%v\n", err)
		return err
	}

	svc := s3.New(sess)
	_, err = svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(loca.BucketName), Key: aws.String(objKey)})
	if err != nil {
		log.Logf("Unable to delete object[key:%s] from bucket %s, %v\n", objKey, loca.BucketName, err)
	}

	err = svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(loca.BucketName),
		Key:    aws.String(objKey),
	})
	if err != nil {
		log.Logf("Error occurred while waiting for object[key:%s] to be deleted.\n", objKey)
	}

	log.Logf("Object[key:%s] successfully deleted\n", objKey)
	return err
}

func ListObjs(loca *LocationInfo) ([]*s3.Object, error) {
	s3c := s3Cred{ak: loca.Access, sk: loca.Security}
	creds := credentials.NewCredentials(&s3c)
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(loca.Region),
		Endpoint:    aws.String(loca.EndPoint),
		Credentials: creds,
	})
	if err != nil {
		log.Logf("[s3mover] New session failed, err:%v\n", err)
		return nil,err
	}

	svc := s3.New(sess)
	input := &s3.ListObjectsInput{Bucket:aws.String(loca.BucketName)}
	output, e := svc.ListObjects(input)
	if e != nil {
		log.Logf("[s3mover] List aws bucket failed, err:%v\n", e)
		return nil,e
	}

	objs := output.Contents
	for ; *output.IsTruncated == true ; {
		input.Marker = output.NextMarker
		output, err = svc.ListObjects(input)
		if err != nil {
			log.Logf("[s3mover] List objects failed, err:%v\n", err)
			return nil,err
		}
		objs = append(objs, output.Contents...)
	}

	log.Logf("[s3mover] Number of objects in bucket[%s] is %d.\n", loca.BucketName, len(objs))
	return output.Contents,nil
}