// Copyright (c) 2018 Click2Cloud Inc. All Rights Reserved.
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

package ceph

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"fmt"

	"io"
	"io/ioutil"
	"time"

	"github.com/micro/go-log"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	pb "github.com/opensds/multi-cloud/s3/proto"
	"github.com/webrtcn/s3client"
	. "github.com/webrtcn/s3client"
	"github.com/webrtcn/s3client/models"
)

type CephAdapter struct {
	backend *backendpb.BackendDetail
	session *s3client.Client
}

func Init(backend *backendpb.BackendDetail) *CephAdapter {
	endpoint := backend.Endpoint
	AccessKeyID := backend.Access
	AccessKeySecret := backend.Security
	sess := s3client.NewClient(endpoint, AccessKeyID, AccessKeySecret)
	adap := &CephAdapter{backend: backend, session: sess}
	return adap
}

func md5Content(data []byte) string {
	md5Ctx := md5.New()
	md5Ctx.Write(data)
	cipherStr := md5Ctx.Sum(nil)
	value := base64.StdEncoding.EncodeToString(cipherStr)
	return value
}

func (ad *CephAdapter) PUT(stream io.Reader, object *pb.Object, ctx context.Context) S3Error {
	bucketName := ad.backend.BucketName

	newObjectKey := object.BucketName + "/" + object.ObjectKey

	if ctx.Value("operation") == "upload" {
		bucket := ad.session.NewBucket()

		cephObject := bucket.NewObject(bucketName)

		d, err := ioutil.ReadAll(stream)
		data := []byte(d)
		contentMD5 := md5Content(data)
		length := int64(len(d))
		body := ioutil.NopCloser(bytes.NewReader(data))

		err = cephObject.Create(newObjectKey, contentMD5, "", length, body, models.Private)

		if err != nil {
			log.Logf("Upload to ceph failed:%v", err)
			return S3Error{Code: 500, Description: "Upload to ceph failed"}
		} else {
			object.LastModified = time.Now().String()[:19]
			log.Logf("LastModified is:%v\n", object.LastModified)
		}
	}

	return NoError
}

func (ad *CephAdapter) GET(object *pb.Object, context context.Context, start int64, end int64) (io.ReadCloser, S3Error) {
	newObjectKey := object.BucketName + "/" + object.ObjectKey

	getObjectOption := GetObjectOption{}
	if start != 0 || end != 0 {
		rangeObj := Range{
			Begin: start,
			End:   end,
		}
		getObjectOption = GetObjectOption{
			Range: &rangeObj,
		}
	}

	if context.Value("operation") == "download" {
		bucket := ad.session.NewBucket()

		cephObject := bucket.NewObject(ad.backend.BucketName)

		getObject, err := cephObject.Get(newObjectKey, &getObjectOption)
		if err != nil {
			fmt.Println(err)
			log.Logf("Download failed:%v", err)
			return nil, S3Error{Code: 500, Description: "Download failed"}
		} else {
			log.Logf("Download succeed, bytes:%d\n", getObject.ContentLength)

			return getObject.Body, NoError
		}
	}

	return nil, NoError
}

func (ad *CephAdapter) DELETE(object *pb.DeleteObjectInput, ctx context.Context) S3Error {

	bucket := ad.session.NewBucket()

	newObjectKey := object.Bucket + "/" + object.Key

	cephObject := bucket.NewObject(ad.backend.BucketName)

	err := cephObject.Remove(newObjectKey)

	if err != nil {
		log.Logf("Delete object failed, err:%v\n", err)
		return InternalError
	}

	log.Logf("Delete object %s from ceph successfully.\n", newObjectKey)

	return NoError
}

func (ad *CephAdapter) GetObjectInfo(bucketName string, key string, context context.Context) (*pb.Object, S3Error) {

	bucket := ad.backend.BucketName
	newKey := bucketName + "/" + key

	bucketO := ad.session.NewBucket()
	bucketResp, err := bucketO.Get(bucket, newKey, "", "", 1000)

	if err != nil {
		log.Logf("Error occured during get Object Info, err:%v\n", err)
		//log.Fatalf("Error occured during get Object Info, err:%v\n", err)
		return nil, S3Error{Code: 500, Description: err.Error()}
	}

	for _, content := range bucketResp.Contents {
		realKey := bucketName + "/" + key
		if realKey != content.Key {
			break
		}
		obj := &pb.Object{
			BucketName: bucketName,
			ObjectKey:  key,
			Size:       content.Size,
		}

		return obj, NoError
	}

	log.Logf("Can not find specified object(%s).\n", key)
	return nil, NoSuchObject
}

func (ad *CephAdapter) InitMultipartUpload(object *pb.Object, context context.Context) (*pb.MultipartUpload, S3Error) {
	bucket := ad.session.NewBucket()
	newObjectKey := object.BucketName + "/" + object.ObjectKey
	log.Logf("bucket = %v,newObjectKey = %v\n", bucket, newObjectKey)
	cephObject := bucket.NewObject(ad.backend.BucketName)
	uploader := cephObject.NewUploads(newObjectKey)
	multipartUpload := &pb.MultipartUpload{}

	res, err := uploader.Initiate(nil)

	if err != nil {
		log.Fatalf("Init s3 multipart upload failed, err:%v\n", err)
		return nil, S3Error{Code: 500, Description: err.Error()}
	} else {
		log.Logf("Init s3 multipart upload succeed, UploadId:%s\n", res.UploadID)
		multipartUpload.Bucket = object.BucketName
		multipartUpload.Key = object.ObjectKey
		multipartUpload.UploadId = res.UploadID
		return multipartUpload, NoError
	}
}

func (ad *CephAdapter) UploadPart(stream io.Reader,
	multipartUpload *pb.MultipartUpload,
	partNumber int64, upBytes int64,
	context context.Context) (*model.UploadPartResult, S3Error) {
	tries := 1
	newObjectKey := multipartUpload.Bucket + "/" + multipartUpload.Key
	bucket := ad.session.NewBucket()

	cephObject := bucket.NewObject(ad.backend.BucketName)
	uploader := cephObject.NewUploads(newObjectKey)
	for tries <= 3 {
		d, err := ioutil.ReadAll(stream)
		data := []byte(d)
		body := ioutil.NopCloser(bytes.NewReader(data))
		contentMD5 := md5Content(data)
		//length := int64(len(data))
		part, err := uploader.UploadPart(int(partNumber), multipartUpload.UploadId, contentMD5, "", upBytes, body)

		if err != nil {
			if tries == 3 {
				log.Logf("[ERROR]Upload part to ceph failed. err:%v\n", err)
				return nil, S3Error{Code: 500, Description: "Upload failed"}
			}
			log.Logf("Retrying to upload part#%d ,err:%s\n", partNumber, err)
			tries++
		} else {
			log.Logf("Uploaded part #%d, ETag:%s\n", partNumber, part.Etag)
			result := &model.UploadPartResult{
				Xmlns:      model.Xmlns,
				ETag:       part.Etag,
				PartNumber: partNumber}
			return result, NoError
		}
	}

	return nil, NoError
}

func (ad *CephAdapter) CompleteMultipartUpload(multipartUpload *pb.MultipartUpload,
	completeUpload *model.CompleteMultipartUpload,
	context context.Context) (*model.CompleteMultipartUploadResult, S3Error) {
	newObjectKey := multipartUpload.Bucket + "/" + multipartUpload.Key
	bucket := ad.session.NewBucket()
	cephObject := bucket.NewObject(ad.backend.BucketName)
	uploader := cephObject.NewUploads(newObjectKey)
	var completeParts []CompletePart
	for _, p := range completeUpload.Part {
		completePart := CompletePart{
			Etag:       p.ETag,
			PartNumber: int(p.PartNumber),
		}
		completeParts = append(completeParts, completePart)
	}
	resp, err := uploader.Complete(multipartUpload.UploadId, completeParts)
	if err != nil {
		log.Logf("completeMultipartUploadS3 failed, err:%v\n", err)
		return nil, S3Error{Code: 500, Description: err.Error()}
	}
	result := &model.CompleteMultipartUploadResult{
		Xmlns:    model.Xmlns,
		Location: ad.backend.Endpoint,
		Bucket:   multipartUpload.Bucket,
		Key:      multipartUpload.Key,
		ETag:     resp.Etag,
	}

	log.Logf("completeMultipartUploadS3 successful, resp:%v\n", resp)
	return result, NoError
}

func (ad *CephAdapter) AbortMultipartUpload(multipartUpload *pb.MultipartUpload, context context.Context) S3Error {
	newObjectKey := multipartUpload.Bucket + "/" + multipartUpload.Key
	bucket := ad.session.NewBucket()
	cephObject := bucket.NewObject(ad.backend.BucketName)
	uploader := cephObject.NewUploads(newObjectKey)
	err := uploader.RemoveUploads(multipartUpload.UploadId)

	if err != nil {
		log.Logf("abortMultipartUploadS3 failed, err:%v\n", err)
		return S3Error{Code: 500, Description: err.Error()}
	} else {
		log.Logf("abortMultipartUploadS3 successful\n")
	}
	return NoError
}

func (ad *CephAdapter) ListParts(listParts *pb.ListParts, context context.Context) (*model.ListPartsOutput, S3Error) {
	newObjectKey := listParts.Bucket + "/" + listParts.Key
	bucket := ad.session.NewBucket()
	cephObject := bucket.NewObject(ad.backend.BucketName)
	uploader := cephObject.NewUploads(newObjectKey)

	listPartsResult, err := uploader.ListPart(listParts.UploadId)
	if err != nil {
		log.Logf("List parts failed, err:%v\n", err)
		return nil, S3Error{Code: 500, Description: err.Error()}
	} else {
		log.Logf("List parts successful\n")
		var parts []model.Part
		for _, p := range listPartsResult.Parts {
			part := model.Part{
				ETag:       p.Etag,
				PartNumber: int64(p.PartNumber),
			}
			parts = append(parts, part)
		}
		listPartsOutput := &model.ListPartsOutput{
			Xmlns:       model.Xmlns,
			Key:         listPartsResult.Key,
			Bucket:      listParts.Bucket,
			IsTruncated: listPartsResult.IsTruncated,
			MaxParts:    listPartsResult.MaxParts,
			Owner: model.Owner{
				ID:          listPartsResult.Owner.OwnerID,
				DisplayName: listPartsResult.Owner.DisplayName,
			},
			UploadId: listPartsResult.UploadID,
			Parts:    parts,
		}

		return listPartsOutput, NoError
	}
}
