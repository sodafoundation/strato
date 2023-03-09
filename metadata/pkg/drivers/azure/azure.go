// Copyright 2023 The SODA Authors.
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

package azure

import (
	"context"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/Azure/azure-storage-blob-go/azblob"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	"github.com/opensds/multi-cloud/metadata/pkg/db"
	"github.com/opensds/multi-cloud/metadata/pkg/model"
	pb "github.com/opensds/multi-cloud/metadata/proto"
)

type AzureAdapter struct {
	backend    *backendpb.BackendDetail
	serviceURL azblob.ServiceURL
}

var ObjectList = func(containerURL azblob.ContainerURL, bucketName string) ([]*model.MetaObject, int64, error) {

	var numObjects int = 0
	var totSize int64 = 0
	objectArray := []*model.MetaObject{}

	blobResponse, err := containerURL.ListBlobsFlatSegment(context.Background(), azblob.Marker{}, azblob.ListBlobsSegmentOptions{})

	if err != nil {
		log.Errorf("unable to list objects. failed with error: %v", err)
	}

	for _, blob := range blobResponse.Segment.BlobItems {
		numObjects += 1
		blobURL := containerURL.NewBlobURL(blob.Name)
		obj := &model.MetaObject{}
		objectArray = append(objectArray, obj)
		obj.ObjectName = blob.Name
		obj.LastModifiedDate = &blob.Properties.LastModified
		obj.Size = *blob.Properties.ContentLength
		totSize += obj.Size
		obj.StorageClass = string(blob.Properties.AccessTier)

		if blob.Properties.DestinationSnapshot != nil {
			obj.RedirectLocation = *blob.Properties.DestinationSnapshot
		}

		if blob.VersionID != nil {
			obj.VersionId = *blob.VersionID
		}

		tagset := map[string]string{}

		if blob.BlobTags != nil {
			for _, tag := range blob.BlobTags.BlobTagSet {
				tagset[tag.Key] = tag.Value
			}
			obj.ObjectTags = tagset
		}

		obj.ExpiresDate = blob.Properties.ExpiresOn

		if blob.Properties.EncryptionScope != nil {
			obj.ServerSideEncryption = *blob.Properties.EncryptionScope
		}

		obj.ReplicationStatus = string(blob.Properties.CopyStatus)

		if blob.Properties.ContentType != nil {
			obj.ObjectType = *blob.Properties.ContentType
		}
		props, err := blobURL.GetProperties(context.Background(), azblob.BlobAccessConditions{}, azblob.ClientProvidedKeyOptions{})
		if err != nil {
			log.Error("unable to get properties for object %v. failed with error: %v", obj.ObjectName, err)
		}

		obj.Metadata = props.NewMetadata()
	}

	return objectArray, totSize, nil

}

func GetBucketMeta(idx int, container azblob.ContainerItem, serviceURL azblob.ServiceURL, bucketArray []*model.MetaBucket, wg *sync.WaitGroup) {

	defer wg.Done()

	buck := &model.MetaBucket{}
	bucketArray[idx] = buck
	buck.Name = container.Name
	buck.CreationDate = &container.Properties.LastModified

	containerURL := serviceURL.NewContainerURL(buck.Name)
	objectArray, totalSize, err := ObjectList(containerURL, buck.Name)

	if err != nil {
		return
	}

	buck.Objects = objectArray
	buck.NumberOfObjects = len(objectArray)
	buck.TotalSize = totalSize

	bucketArray[idx] = buck

	props, err := containerURL.GetProperties(context.Background(), azblob.LeaseAccessConditions{})

	if err != nil {
		log.Errorf("unable to get bucket tags. failed wth error: %v", err)
	} else {
		buck.BucketTags = props.NewMetadata()
	}

	acl, err := containerURL.GetAccessPolicy(context.Background(), azblob.LeaseAccessConditions{})

	if err != nil {
		log.Errorf("unable to get bucket Acl. failed with error: %v", err)
	} else {
		access := []*model.Access{}

		for _, item := range acl.Items {
			acc := &model.Access{}
			acc.ID = item.ID
			acc.Permission = *item.AccessPolicy.Permission
			access = append(access, acc)
		}
		buck.BucketAcl = access
	}
}

func BucketList(serviceURL azblob.ServiceURL) ([]*model.MetaBucket, error) {
	response, err := serviceURL.ListContainersSegment(context.Background(), azblob.Marker{}, azblob.ListContainersSegmentOptions{})

	if err != nil {
		log.Errorf("unable to list buckets. failed with err: %v", err)
	}

	bucketArray := make([]*model.MetaBucket, len(response.ContainerItems))

	wg := sync.WaitGroup{}
	for idx, container := range response.ContainerItems {
		wg.Add(1)
		go GetBucketMeta(idx, container, serviceURL, bucketArray, &wg)
	}
	wg.Wait()

	return bucketArray, nil
}

func (ad *AzureAdapter) SyncMetadata(ctx context.Context, in *pb.SyncMetadataRequest) error {

	buckArr, err := BucketList(ad.serviceURL)
	if err != nil {
		log.Errorf("metadata collection for backend id: %v failed with error: %v", ad.backend.Id, err)
		return err
	}

	metaBackend := model.MetaBackend{}
	metaBackend.Id = ad.backend.Id
	metaBackend.BackendName = ad.backend.Name
	metaBackend.Type = ad.backend.Type
	metaBackend.Region = ad.backend.Region
	metaBackend.Buckets = buckArr
	metaBackend.NumberOfBuckets = int32(len(buckArr))
	newContext := context.TODO()
	err = db.DbAdapter.CreateMetadata(newContext, metaBackend)

	return err
}

func (ad *AzureAdapter) DownloadObject() {

}
