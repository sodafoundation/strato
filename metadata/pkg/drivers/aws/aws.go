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

package aws

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	"github.com/opensds/multi-cloud/metadata/pkg/db"
	"github.com/opensds/multi-cloud/metadata/pkg/model"
	pb "github.com/opensds/multi-cloud/metadata/proto"
	log "github.com/sirupsen/logrus"
)

type S3Cred struct {
	Ak string
	Sk string
}

func (myc *S3Cred) IsExpired() bool {
	return false
}

func (myc *S3Cred) Retrieve() (credentials.Value, error) {
	cred := credentials.Value{AccessKeyID: myc.Ak, SecretAccessKey: myc.Sk}
	return cred, nil
}

type AwsAdapter struct {
	Backend *backendpb.BackendDetail
	Session *session.Session
}

func ObjectList(sess *session.Session, bucket *model.MetaBucket) error {
	svc := s3.New(sess)
	output, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: &bucket.Name})
	if err != nil {
		return err
	}

	numObjects := len(output.Contents)

	objectArray := make([]model.MetaObject, numObjects)
	wg := sync.WaitGroup{}
	var totSize int64 = 0
	for i, object := range output.Contents {
		wg.Add(1)
		go func(i int, object *s3.Object) {
			defer wg.Done()
			obj := model.MetaObject{}
			obj.LastModifiedDate = *object.LastModified
			obj.ObjectName = *object.Key
			obj.Size = *object.Size
			totSize += obj.Size
			obj.StorageClass = *object.StorageClass

			meta, _ := svc.HeadObject(&s3.HeadObjectInput{Bucket: &bucket.Name, Key: object.Key})
			if meta.ServerSideEncryption != nil {
				obj.ServerSideEncryption = *meta.ServerSideEncryption
			}
			if meta.VersionId != nil {
				obj.VersionId = *meta.VersionId
			}
			obj.ObjectType = *meta.ContentType
			if meta.Expires != nil {
				obj.ExpiresDate, _ = time.Parse(time.RFC3339, *meta.Expires)
			}
			if meta.ReplicationStatus != nil {
				obj.ReplicationStatus = *meta.ReplicationStatus
			}
			objectArray[i] = obj
		}(i, object)
	}
	wg.Wait()
	bucket.NumberOfObjects = numObjects
	bucket.TotalSize = totSize
	bucket.Objects = objectArray
	return err
}

func BucketList(sess *session.Session) ([]model.MetaBucket, error) {
	svc := s3.New(sess)

	output, err := svc.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		log.Fatal(err)
	}
	numBuckets := len(output.Buckets)
	bucketArray := make([]model.MetaBucket, numBuckets)
	wg := sync.WaitGroup{}
	for i, bucket := range output.Buckets {
		wg.Add(1)
		go func(i int, bucket *s3.Bucket) {
			defer wg.Done()
			buck := model.MetaBucket{}
			buck.CreationDate = *bucket.CreationDate
			buck.Name = *bucket.Name
			loc, _ := svc.GetBucketLocation(&s3.GetBucketLocationInput{Bucket: bucket.Name})
			buck.Region = *loc.LocationConstraint
			tags, _ := svc.GetBucketTagging(&s3.GetBucketTaggingInput{Bucket: bucket.Name})
			tagset := make(map[string]string)
			for _, tag := range tags.TagSet {
				tagset[*tag.Key] = *tag.Value
			}
			buck.BucketTags = tagset
			newSess := sess
			newSess.Config.Region = &buck.Region
			err := ObjectList(newSess, &buck)
			if err != nil {
				log.Fatal(err)
			}
			bucketArray[i] = buck
		}(i, bucket)
	}
	wg.Wait()
	return bucketArray, err
}

func (ad *AwsAdapter) SyncMetadata(ctx context.Context, in *pb.SyncMetadataRequest) error {

	buckArr, _ := BucketList(ad.Session)
	db.DbAdapter.CreateMetadata(ctx, ad.Backend, buckArr)
	return nil
}

func (ad *AwsAdapter) DownloadObject() {
	log.Info("Implement me")
}
