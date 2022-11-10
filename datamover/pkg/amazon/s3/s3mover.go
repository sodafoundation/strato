// Copyright 2019 The soda Authors.
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
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	log "github.com/sirupsen/logrus"

	. "github.com/soda/multi-cloud/datamover/pkg/utils"
	pb "github.com/soda/multi-cloud/datamover/proto"
)

type S3Mover struct {
	downloader         *s3manager.Downloader           //for multipart download
	svc                *s3.S3                          //for multipart upload
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

func handleAWSS3Errors(err error) error {
	if err != nil {
		if serr, ok := err.(awserr.Error); ok { // This error is a Service-specific
			switch serr.Code() { // Compare serviceCode to ServiceCodeXxx constants
			case "InvalidAccessKeyId":
				log.Info("aws s3 error: permission denied.")
				return errors.New(DMERR_NoPermission)
			case "NoSuchUpload":
				return errors.New(DMERR_NoSuchUpload)
			default:
				return err
			}
		}
		return err
	}

	return nil
}

func (mover *S3Mover) UploadObj(objKey string, destLoca *LocationInfo, buf []byte) error {
	log.Infof("[s3mover] UploadObj object, key:%s.", objKey)
	s3c := s3Cred{ak: destLoca.Access, sk: destLoca.Security}
	creds := credentials.NewCredentials(&s3c)
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(destLoca.Region),
		Endpoint:    aws.String(destLoca.EndPoint),
		Credentials: creds,
	})
	if err != nil {
		log.Errorf("[s3mover] New session failed, err:%v\n", err)
		return handleAWSS3Errors(err)
	}

	reader := bytes.NewReader(buf)
	uploader := s3manager.NewUploader(sess)
	log.Infof("[s3mover] Try to upload, bucket:%s,obj:%s\n", destLoca.BucketName, objKey)
	input := s3manager.UploadInput{
		Bucket: aws.String(destLoca.BucketName),
		Key:    aws.String(objKey),
		Body:   reader,
	}
	if destLoca.ClassName != "" {
		input.StorageClass = aws.String(destLoca.ClassName)
	}
	for tries := 1; tries <= 3; tries++ {
		_, err = uploader.Upload(&input)
		if err != nil {
			log.Errorf("[s3mover] Upload object[%s] failed %d times, err:%v\n", objKey, tries, err)
			e := handleAWSS3Errors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission { //If no permission, then no need to retry.
				return e
			}
		} else {
			log.Infof("[s3mover] Upload object[%s] successfully.", objKey)
			return nil
		}
	}

	log.Infof("[s3mover] Upload object, bucket:%s,obj:%s, should not be here.\n", destLoca.BucketName, objKey)
	return errors.New(DMERR_InternalError)
}

func (mover *S3Mover) DownloadObj(objKey string, srcLoca *LocationInfo, buf []byte) (size int64, err error) {
	s3c := s3Cred{ak: srcLoca.Access, sk: srcLoca.Security}
	creds := credentials.NewCredentials(&s3c)
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(srcLoca.Region),
		Endpoint:    aws.String(srcLoca.EndPoint),
		Credentials: creds,
	})
	if err != nil {
		log.Errorf("[s3mover] New session failed, err:%v\n", err)
		return 0, handleAWSS3Errors(err)
	}

	writer := aws.NewWriteAtBuffer(buf)
	downLoader := s3manager.NewDownloader(sess)
	getObjInput := s3.GetObjectInput{
		Bucket: aws.String(srcLoca.BucketName),
		Key:    aws.String(objKey),
	}
	log.Infof("[s3mover] Try to download, bucket:%s,obj:%s\n", srcLoca.BucketName, objKey)
	for tries := 1; tries <= 3; tries++ {
		numBytes, err := downLoader.Download(writer, &getObjInput)
		if err != nil {
			log.Errorf("[s3mover]download object[bucket:%s,key:%s] failed %d times, err:%v\n",
				srcLoca.BucketName, objKey, tries, err)
			e := handleAWSS3Errors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission { //If no permission, then no need to retry.
				return 0, e
			}
		} else {
			log.Infof("[s3mover]downlad object[bucket:%s,key:%s] succeed, bytes:%d\n", srcLoca.BucketName, objKey, numBytes)
			return numBytes, nil
		}
	}

	log.Infof("[s3mover]downlad object[bucket:%s,key:%s], should not be here.\n", srcLoca.BucketName, objKey)
	return 0, errors.New(DMERR_InternalError)
}

func (mover *S3Mover) MultiPartDownloadInit(srcLoca *LocationInfo) error {
	s3c := s3Cred{ak: srcLoca.Access, sk: srcLoca.Security}
	creds := credentials.NewCredentials(&s3c)
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(srcLoca.Region),
		Endpoint:    aws.String(srcLoca.EndPoint),
		Credentials: creds,
	})
	if err != nil {
		log.Errorf("[s3mover] new session for multipart download failed, err:%v\n", err)
		return handleAWSS3Errors(err)
	}

	mover.downloader = s3manager.NewDownloader(sess)
	log.Infof("[s3mover] MultiPartDownloadInit succeed.")
	return nil
}

func (mover *S3Mover) DownloadRange(objKey string, srcLoca *LocationInfo, buf []byte, start int64, end int64) (size int64, err error) {
	log.Infof("[s3mover] Download object[%s] range[%d - %d]...\n", objKey, start, end)
	writer := aws.NewWriteAtBuffer(buf)
	getObjInput := s3.GetObjectInput{
		Bucket: aws.String(srcLoca.BucketName),
		Key:    aws.String(objKey),
	}
	strStart := strconv.FormatInt(start, 10)
	strEnd := strconv.FormatInt(end, 10)
	rg := "bytes=" + strStart + "-" + strEnd
	getObjInput.SetRange(rg)
	log.Infof("[s3mover] Try to download object:%s, range:=%s\n", objKey, rg)
	for tries := 1; tries <= 3; tries++ {
		numBytes, err := mover.downloader.Download(writer, &getObjInput)
		if err != nil {
			log.Errorf("[s3mover] download object[%s] range[%d - %d] failed %d times, err:%v\n",
				objKey, start, end, tries, err)
			e := handleAWSS3Errors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission { //If no permission, then no need to retry.
				return 0, e
			}
		} else {
			log.Infof("[s3mover] download object[%s] range[%d - %d] succeed, bytes:%d\n", objKey, start, end, numBytes)
			return numBytes, nil
		}
	}

	log.Infof("[s3mover] download object[%s] range[%d - %d], should not be here.\n", objKey, start, end)
	return 0, errors.New(DMERR_InternalError)
}

func (mover *S3Mover) MultiPartUploadInit(objKey string, destLoca *LocationInfo) (string, error) {
	s3c := s3Cred{ak: destLoca.Access, sk: destLoca.Security}
	creds := credentials.NewCredentials(&s3c)
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(destLoca.Region),
		Endpoint:    aws.String(destLoca.EndPoint),
		Credentials: creds,
	})
	if err != nil {
		log.Errorf("[s3mover] new session failed, err:%v\n", err)
		return "", handleAWSS3Errors(err)
	}

	mover.svc = s3.New(sess)
	multiUpInput := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(destLoca.BucketName),
		Key:    aws.String(objKey),
	}
	if destLoca.ClassName != "" {
		multiUpInput.StorageClass = aws.String(destLoca.ClassName)
	}
	log.Infof("[s3mover] Try to init multipart upload[objkey:%s].\n", objKey)
	for tries := 1; tries <= 3; tries++ {
		resp, err := mover.svc.CreateMultipartUpload(multiUpInput)
		if err != nil {
			log.Errorf("[s3mover] init multipart upload[objkey:%s] failed %d times, err:%v.\n", objKey, tries, err)
			e := handleAWSS3Errors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission { //If no permission, then no need to retry.
				return "", e
			}
		} else {
			mover.multiUploadInitOut = resp
			log.Infof("[s3mover] init multipart upload[objkey:%s] successfully, UploadId:%s\n", objKey, *resp.UploadId)
			return "", nil
		}
	}

	log.Infof("[s3mover] init multipart upload[objkey:%s], should not be here.\n", objKey)
	return *mover.multiUploadInitOut.UploadId, errors.New(DMERR_InternalError)
}

func (mover *S3Mover) UploadPart(objKey string, destLoca *LocationInfo, upBytes int64, buf []byte, partNumber int64, offset int64) error {
	log.Infof("[s3mover] Upload range[objkey:%s, partnumber#%d,offset#%d,upBytes#%d,uploadid#%s]...\n", objKey, partNumber,
		offset, upBytes, *mover.multiUploadInitOut.UploadId)

	upPartInput := &s3.UploadPartInput{
		Body:          bytes.NewReader(buf),
		Bucket:        aws.String(destLoca.BucketName),
		Key:           aws.String(objKey),
		PartNumber:    aws.Int64(partNumber),
		UploadId:      aws.String(*mover.multiUploadInitOut.UploadId),
		ContentLength: aws.Int64(upBytes),
	}

	log.Infof("[s3mover] Try to upload range[objkey:%s, partnumber#%d, offset#%d].\n", objKey, partNumber, offset)
	for tries := 1; tries <= 3; tries++ {
		upRes, err := mover.svc.UploadPart(upPartInput)
		if err != nil {
			log.Errorf("[s3mover] upload range[objkey:%s, partnumber#%d, offset#%d] failed %d times, err:%v\n",
				objKey, partNumber, offset, tries, err)
			e := handleAWSS3Errors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission { //If no permission, then no need to retry.
				return e
			}
		} else {
			part := s3.CompletedPart{
				ETag:       upRes.ETag,
				PartNumber: aws.Int64(partNumber),
			}
			mover.completeParts = append(mover.completeParts, &part)
			log.Infof("[s3mover] Upload range[objkey:%s, partnumber#%d,offset#%d] successfully.\n", objKey, partNumber, offset)
			return nil
		}
	}

	log.Infof("[s3mover] upload range[objkey:%s, partnumber#%d, offset#%d], should not be here.\n", objKey, partNumber, offset)
	return errors.New(DMERR_InternalError)
}

func (mover *S3Mover) AbortMultipartUpload(objKey string, destLoca *LocationInfo) error {
	log.Infof("[s3mover] Abort multipart upload[objkey:%s] for uploadId#%s.\n", objKey, *mover.multiUploadInitOut.UploadId)
	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(destLoca.BucketName),
		Key:      aws.String(objKey),
		UploadId: aws.String(*mover.multiUploadInitOut.UploadId),
	}

	for tries := 1; tries <= 3; tries++ {
		_, err := mover.svc.AbortMultipartUpload(abortInput)
		if err != nil {
			log.Errorf("[s3mover] abort multipart upload[objkey:%s] for uploadId#%s failed %d times, err:%v.\n",
				objKey, *mover.multiUploadInitOut.UploadId, tries, err)
			e := handleAWSS3Errors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission { //If no permission, then no need to retry.
				return e
			}
		} else {
			log.Infof("[s3mover] abort multipart upload[objkey:%s] for uploadId#%s successfully.\n",
				objKey, *mover.multiUploadInitOut.UploadId, tries)
			return nil
		}
	}
	log.Infof("[s3mover] abort multipart upload[objkey:%s] for uploadId#%s, should not be here.\n",
		objKey, *mover.multiUploadInitOut.UploadId)
	return errors.New(DMERR_InternalError)
}

func (mover *S3Mover) CompleteMultipartUpload(objKey string, destLoca *LocationInfo) error {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(destLoca.BucketName),
		Key:      aws.String(objKey),
		UploadId: aws.String(*mover.multiUploadInitOut.UploadId),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: mover.completeParts,
		},
	}

	log.Infof("[s3mover] Try to do CompleteMultipartUpload [objkey:%s].\n", objKey)
	for tries := 1; tries <= 3; tries++ {
		rsp, err := mover.svc.CompleteMultipartUpload(completeInput)
		if err != nil {
			log.Errorf("[s3mover] completeMultipartUpload [objkey:%s] failed %d times, err:%v\n", objKey, tries, err)
			e := handleAWSS3Errors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission { //If no permission, then no need to retry.
				return e
			}
		} else {
			log.Infof("[s3mover] completeMultipartUpload successfully [objkey:%s], rsp:%v\n", objKey, rsp)
			return nil
		}
	}

	log.Infof("[s3mover] completeMultipartUpload [objkey:%s], should not be here.\n", objKey)
	return errors.New(DMERR_InternalError)
}

func (mover *S3Mover) DeleteObj(objKey string, loca *LocationInfo) error {
	s3c := s3Cred{ak: loca.Access, sk: loca.Security}
	creds := credentials.NewCredentials(&s3c)
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(loca.Region),
		Endpoint:    aws.String(loca.EndPoint),
		Credentials: creds,
	})
	if err != nil {
		log.Errorf("[s3mover] new session failed, err:%v\n", err)
		return err
	}

	svc := s3.New(sess)
	log.Infof("[s3mover] Try to delete object[key:%s] from bucket %s.\n", objKey, loca.BucketName)
	for tries := 1; tries <= 3; tries++ {
		_, err = svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(loca.BucketName), Key: aws.String(objKey)})
		if err != nil {
			log.Errorf("[s3mover] delete object[key:%s] from bucket %s failed %d times, err:%v\n",
				objKey, loca.BucketName, tries, err)
			e := handleAWSS3Errors(err)
			if tries >= 3 || e.Error() == DMERR_NoPermission { //If no permission, then no need to retry.
				return e
			}
		} else {
			err = svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
				Bucket: aws.String(loca.BucketName),
				Key:    aws.String(objKey),
			})
			if err != nil {
				log.Errorf("[s3mover] error occurred while waiting for object[%s] to be deleted.\n", objKey)
			} else {
				log.Infof("[s3mover] delete object[key:%s] from bucket %s successfully.\n", objKey, loca.BucketName)
			}
			return err
		}
	}

	log.Infof("[s3mover] Delete Object[%s], should not be here.\n", objKey)
	return errors.New(DMERR_InternalError)
}

func ListObjs(loca *LocationInfo, filt *pb.Filter) ([]*s3.Object, error) {
	s3c := s3Cred{ak: loca.Access, sk: loca.Security}
	creds := credentials.NewCredentials(&s3c)
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(loca.Region),
		Endpoint:    aws.String(loca.EndPoint),
		Credentials: creds,
	})
	if err != nil {
		log.Errorf("[s3mover] new session failed, err:%v\n", err)
		return nil, handleAWSS3Errors(err)
	}

	svc := s3.New(sess)
	input := &s3.ListObjectsInput{Bucket: aws.String(loca.BucketName)}
	if filt != nil {
		input.Prefix = &filt.Prefix
	}
	output, err := svc.ListObjects(input)
	e := handleAWSS3Errors(err)
	if e != nil {
		log.Errorf("[s3mover] list aws bucket failed, err:%v\n", e)
		return nil, e
	}

	objs := output.Contents
	for *output.IsTruncated == true {
		input.Marker = output.NextMarker
		output, err = svc.ListObjects(input)
		e := handleAWSS3Errors(err)
		if e != nil {
			log.Errorf("[s3mover] list objects failed, err:%v\n", e)
			return nil, e
		}
		objs = append(objs, output.Contents...)
	}

	log.Infof("[s3mover] Number of objects in bucket[%s] is %d.\n", loca.BucketName, len(objs))
	return output.Contents, nil
}
