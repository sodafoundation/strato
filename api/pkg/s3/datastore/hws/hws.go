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

package hws

import (
	"context"
	"io"
	"time"

	"github.com/micro/go-log"
	"github.com/opensds/multi-cloud/api/pkg/utils/obs"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	pb "github.com/opensds/multi-cloud/s3/proto"
)

type OBSAdapter struct {
	backend *backendpb.BackendDetail
	client  *obs.ObsClient
}

func Init(backend *backendpb.BackendDetail) *OBSAdapter {
	endpoint := backend.Endpoint
	AccessKeyID := backend.Access
	AccessKeySecret := backend.Security

	client, err := obs.New(AccessKeyID, AccessKeySecret, endpoint)

	if err != nil {
		log.Logf("Access obs failed:%v", err)
		return nil
	}

	adap := &OBSAdapter{backend: backend, client: client}
	return adap
}

func (ad *OBSAdapter) PUT(stream io.Reader, object *pb.Object, ctx context.Context) S3Error {

	bucket := ad.backend.BucketName
	if ctx.Value("operation") == "upload" {
		input := &obs.PutObjectInput{}
		input.Bucket = bucket
		input.Key = object.BucketName + "/" + object.ObjectKey
		input.Body = stream

		out, err := ad.client.PutObject(input)

		if err != nil {
			log.Logf("Upload to obs failed:%v", err)
			return S3Error{Code: 500, Description: "Upload to obs failed"}

		} else {
			object.LastModified = time.Now().Unix()
			log.Logf("LastModified is:%v\n", object.LastModified)
		}
		log.Logf("Upload %s to obs successfully.", out.VersionId)
	}

	return NoError
}

func (ad *OBSAdapter) GET(object *pb.Object, context context.Context, start int64, end int64) (io.ReadCloser, S3Error) {

	bucket := ad.backend.BucketName
	if context.Value("operation") == "download" {
		input := &obs.GetObjectInput{}
		input.Bucket = bucket
		input.Key = object.BucketName + "/" + object.ObjectKey
		if start != 0 || end != 0 {
			input.RangeStart = start
			input.RangeEnd = end
		}
		out, err := ad.client.GetObject(input)

		if err != nil {
			log.Logf("download hws obs failed:%v", err)
			return nil, S3Error{Code: 500, Description: "download hws obs failed"}
		} else {
			log.Logf("download obs successfully.%v", out.VersionId)
			return out.Body, NoError
		}
	}

	return nil, NoError
}

func (ad *OBSAdapter) DELETE(object *pb.DeleteObjectInput, ctx context.Context) S3Error {

	newObjectKey := object.Bucket + "/" + object.Key
	deleteObjectInput := obs.DeleteObjectInput{Bucket: ad.backend.BucketName, Key: newObjectKey}
	_, err := ad.client.DeleteObject(&deleteObjectInput)
	if err != nil {
		log.Logf("Delete  object failed:%v", err)
		return InternalError
	}

	log.Logf("Delete object %s from obs successfully.\n", newObjectKey)
	return NoError
}

func (ad *OBSAdapter) GetObjectInfo(bucketName string, key string, context context.Context) (*pb.Object, S3Error) {
	return nil, NoError
}

func (ad *OBSAdapter) InitMultipartUpload(object *pb.Object, context context.Context) (*pb.MultipartUpload, S3Error) {
	bucket := ad.backend.BucketName
	multipartUpload := &pb.MultipartUpload{}
	if context.Value("operation") == "multipartupload" {
		input := &obs.InitiateMultipartUploadInput{}
		input.Bucket = bucket
		input.Key = object.BucketName + "/" + object.ObjectKey
		out, err := ad.client.InitiateMultipartUpload(input)
		if err != nil {
			log.Logf("initmultipartupload failed:%v", err)
			return nil, S3Error{Code: 500, Description: "initmultipartupload failed"}
		} else {
			log.Log("initmultipartupload  successfully.")
			multipartUpload.Bucket = out.Bucket
			multipartUpload.Key = out.Key
			multipartUpload.UploadId = out.UploadId
			log.Logf("multipartUpload is %v\n", multipartUpload)
			return multipartUpload, NoError
		}
	}
	return nil, NoError

}

func (ad *OBSAdapter) UploadPart(stream io.Reader, multipartUpload *pb.MultipartUpload, partNumber int64, upBytes int64, context context.Context) (*model.UploadPartResult, S3Error) {

	bucket := ad.backend.BucketName
	newObjectKey := multipartUpload.Bucket + "/" + multipartUpload.Key
	if context.Value("operation") == "multipartupload" {
		input := &obs.UploadPartInput{}
		input.Bucket = bucket
		input.Key = newObjectKey
		input.Body = stream
		input.PartNumber = int(partNumber)
		input.PartSize = upBytes
		log.Logf(" multipartUpload.UploadId is %v", multipartUpload.UploadId)
		input.UploadId = multipartUpload.UploadId
		out, err := ad.client.UploadPart(input)

		if err != nil {
			log.Logf("uploadpart init failed:%v", err)
			return nil, S3Error{Code: 500, Description: "uploadpart init failed"}
		} else {
			log.Logf("uploadpart %v successfully.", out.PartNumber)
			result := &model.UploadPartResult{ETag: out.ETag, PartNumber: partNumber}
			return result, NoError
		}
	}
	return nil, NoError
}

func (ad *OBSAdapter) CompleteMultipartUpload(
	multipartUpload *pb.MultipartUpload,
	completeUpload *model.CompleteMultipartUpload,
	context context.Context) (*model.CompleteMultipartUploadResult, S3Error) {
	log.Logf("enter the hws CompleteMultipartUpload method")
	bucket := ad.backend.BucketName
	log.Logf("bucket is %v\n", bucket)
	newObjectKey := multipartUpload.Bucket + "/" + multipartUpload.Key
	log.Logf("newObjectKey is %v\n", newObjectKey)
	if context.Value("operation") == "multipartupload" {
		input := &obs.CompleteMultipartUploadInput{}
		input.Bucket = bucket
		input.Key = newObjectKey
		input.UploadId = multipartUpload.UploadId
		for _, p := range completeUpload.Part {
			part := obs.Part{
				PartNumber: int(p.PartNumber),
				ETag:       p.ETag,
			}
			input.Parts = append(input.Parts, part)
		}
		resp, err := ad.client.CompleteMultipartUpload(input)
		result := &model.CompleteMultipartUploadResult{
			Xmlns:    model.Xmlns,
			Location: resp.Location,
			Bucket:   resp.Bucket,
			Key:      resp.Key,
			ETag:     resp.ETag,
		}
		if err != nil {
			log.Logf("CompleteMultipartUploadInput is nil:%v", err)
			return nil, S3Error{Code: 500, Description: "uploadpart init failed"}
		}

		log.Logf("CompleteMultipartUploadInput successfully.")
		return result, NoError
	}

	return nil, NoError
}

func (ad *OBSAdapter) AbortMultipartUpload(multipartUpload *pb.MultipartUpload, context context.Context) S3Error {
	bucket := ad.backend.BucketName
	newObjectKey := multipartUpload.Bucket + "/" + multipartUpload.Key
	if context.Value("operation") == "multipartupload" {
		input := &obs.AbortMultipartUploadInput{}
		input.UploadId = multipartUpload.UploadId
		input.Bucket = bucket
		input.Key = newObjectKey
		_, err := ad.client.AbortMultipartUpload(input)
		if err != nil {
			log.Logf("AbortMultipartUploadInput is nil:%v", err)
			return S3Error{Code: 500, Description: "AbortMultipartUploadInput failed"}
		} else {
			log.Logf("AbortMultipartUploadInput successfully.")
			return NoError
		}
	}
	return NoError
}

func (ad *OBSAdapter) ListParts(listParts *pb.ListParts, context context.Context) (*model.ListPartsOutput, S3Error) {
	bucket := ad.backend.BucketName
	if context.Value("operation") == "listParts" {
		input := &obs.ListPartsInput{}
		input.Bucket = bucket
		input.Key = listParts.Key
		input.UploadId = listParts.UploadId
		input.MaxParts = int(listParts.MaxParts)
		listPartsOutput, err := ad.client.ListParts(input)
		listParts := &model.ListPartsOutput{}
		listParts.Bucket = listPartsOutput.Bucket
		listParts.Key = listPartsOutput.Key
		listParts.UploadId = listPartsOutput.UploadId
		listParts.MaxParts = listPartsOutput.MaxParts

		for _, p := range listPartsOutput.Parts {
			part := model.Part{
				PartNumber: int64(p.PartNumber),
				ETag:       p.ETag,
			}
			listParts.Parts = append(listParts.Parts, part)
		}

		if err != nil {
			log.Logf("ListPartsListParts is nil:%v\n", err)
			return nil, S3Error{Code: 500, Description: "AbortMultipartUploadInput failed"}
		} else {
			log.Logf("ListParts successfully")
			return listParts, NoError
		}
	}
	return nil, NoError
}
