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


package backend


import (
	"bytes"
	"context"
	"net/url"
	"time"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/driver"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	pb "github.com/opensds/multi-cloud/s3/proto"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/azure"
	log "github.com/sirupsen/logrus"
)

func azureCheck(ctx context.Context ,backendDetail *backendpb.BackendDetail)(error){


var MaxTimeForSingleHttpRequest = 50 * time.Minute

driver.RegisterDriverFactory(backendDetail.Type, &azure.AzureBlobDriverFactory{})
adap, err := driver.CreateStorageDriver(backendDetail.Type, backendDetail)
	if err != nil {
		log.Debug("failed to create storage. err:", err)
		return err
	}

endpoint := backendDetail.Endpoint
AccessKeyID := backendDetail.Access
AccessKeySecret := backendDetail.Security

credential, err := azblob.NewSharedKeyCredential(AccessKeyID, AccessKeySecret)

	if err != nil {
		log.Debug("create credential[Azure Blob] failed, err:%v\n", err)
		return  err
	}

	//create containerURL
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			TryTimeout: MaxTimeForSingleHttpRequest,
		},
	})
	URL, _ := url.Parse(endpoint)

	containerURL:=azblob.NewContainerURL(*URL, p)

object:= pb.Object{
	BucketName: backendDetail.BucketName,
	ObjectKey: "emptyContainer/",
}
objectId := object.BucketName + "/" + object.ObjectKey
blobURL := containerURL.NewBlockBlobURL(objectId)
bs := []byte{0}
stream:= bytes.NewReader(bs)
options := azblob.UploadStreamToBlockBlobOptions{BufferSize: 2 * 1024 * 1024, MaxBuffers: 2}
_, err= azblob.UploadStreamToBlockBlob(ctx, stream, blobURL, options)
	if err != nil {
		log.Debug("failed to put object[Azure Blob], objectId:%s, err:%v\n", objectId, err)
		return err
	}
//delete bucket
	input:= &pb.DeleteObjectInput{
		Bucket:backendDetail.BucketName,
		Key:"EmptyContainer/",
	}


	err= adap.Delete(ctx, input)
	if err != nil {
		log.Debug("failed to delete object[Azure Blob], objectId:%s, err:%v\n", objectId, err)
		return  err
	}
log.Debug("create and delete object is successful\n")

return nil

}

