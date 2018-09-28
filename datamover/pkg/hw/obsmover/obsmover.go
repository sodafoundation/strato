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
	"fmt"
	"io"
	"obs"

	"github.com/micro/go-log"
	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
)

func DownloadObj(obj *SourceOject, srcLoca *LocationInfo, buf []byte) (size int64, err error) {
	obsClient, err := obs.New(srcLoca.Access, srcLoca.Security, srcLoca.EndPoint)
	if err != nil {
		return 0, err
	}

	input := &obs.GetObjectInput{}
	input.Bucket = srcLoca.BucketName
	input.Key = obj.Obj.ObjectKey

	output, err := obsClient.GetObject(input)
	if err == nil {
		size = 0
		defer output.Body.Close()
		log.Logf("StorageClass:%s, ETag:%s, ContentType:%s, ContentLength:%d, LastModified:%s\n",
			output.StorageClass, output.ETag, output.ContentType, output.ContentLength, output.LastModified)
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
				log.Logf("readErr=%v\n", readErr)
				break
			}
		}
		if readErr == io.EOF {
			readErr = nil
		}
		return size, readErr
	} else if obsError, ok := err.(obs.ObsError); ok {
		log.Logf("Code:%s\n", obsError.Code)
		log.Logf("Message:%s\n", obsError.Message)
		return 0, err
	}
	return
}

func UploadObj(obj *SourceOject, destLoca *LocationInfo, buf []byte) error {
	obsClient, err := obs.New(destLoca.Access, destLoca.Security, destLoca.EndPoint)
	if err != nil {
		return err
	}

	input := &obs.PutObjectInput{}
	input.Bucket = destLoca.BucketName
	input.Key = obj.Obj.ObjectKey
	input.Body = bytes.NewReader(buf)
	output, err := obsClient.PutObject(input)
	if err != nil {
		fmt.Printf("err: %v\n", err)
	} else {
		fmt.Printf("RequestId:%s, ETag:%s\n", output.RequestId, output.ETag)
	}

	return err
}

func DeleteObj(obj *SourceOject, loca *LocationInfo) error {
	obsClient, err := obs.New(loca.Access, loca.Security, loca.EndPoint)
	if err != nil {
		log.Logf("New client failed when delete obj[objKey:%s] in storage backend[type:hws], err:%v\n", obj.Obj.ObjectKey, err)
		return err
	}

	input := &obs.DeleteObjectInput{}
	input.Bucket = loca.BucketName
	input.Key = obj.Obj.ObjectKey

	output, err := obsClient.DeleteObject(input)
	if err == nil {
		log.Logf("Delete object[objKey:%s] in storage backend succeed, RequestId:%s\n", obj.Obj.ObjectKey, output.RequestId)
	} else {
		log.Logf("Delete object[objKey:%s] in storage backend failed, err:%v\n", obj.Obj.ObjectKey, err)
	}

	return err
}
