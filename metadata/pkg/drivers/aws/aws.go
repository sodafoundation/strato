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
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
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

func iterateObjects(i int, object *s3.Object, svc *s3.S3, bucketName string, objectArray []model.MetaObject, wg *sync.WaitGroup) {
	defer wg.Done()
	obj := model.MetaObject{}
	obj.LastModifiedDate = *object.LastModified
	obj.ObjectName = *object.Key
	obj.Size = *object.Size
	obj.StorageClass = *object.StorageClass

	meta, err := svc.HeadObject(&s3.HeadObjectInput{Bucket: &bucketName, Key: object.Key})
	if err != nil {
		log.Errorf("cannot perform head object on object %v in bucket %v. failed with error: %v\n", *object.Key, bucketName, err)
	}
	if meta.ServerSideEncryption != nil {
		obj.ServerSideEncryption = *meta.ServerSideEncryption
	}
	if meta.VersionId != nil {
		obj.VersionId = *meta.VersionId
	}
	obj.ObjectType = *meta.ContentType
	if meta.Expires != nil {
		expiresTime, err := time.Parse(time.RFC3339, *meta.Expires)
		if err != nil {
			log.Errorf("unable to parse given string to time type. error: %v. skipping ExpiresDate field\n", err)
		} else {
			obj.ExpiresDate = expiresTime
		}
	}
	if meta.ReplicationStatus != nil {
		obj.ReplicationStatus = *meta.ReplicationStatus
	}
	objectArray[i] = obj
}

func ObjectList(sess *session.Session, bucket *model.MetaBucket) error {
	svc := s3.New(sess, aws.NewConfig().WithRegion(bucket.Region))
	output, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: &bucket.Name})
	if err != nil {
		log.Errorf("unable to list objects in bucket %v. failed with error: %v\n", bucket.Name, err)
		return err
	}

	numObjects := len(output.Contents)

	objectArray := make([]model.MetaObject, numObjects)
	wg := sync.WaitGroup{}
	for i, object := range output.Contents {
		wg.Add(1)
		go iterateObjects(i, object, svc, bucket.Name, objectArray, &wg)
	}
	wg.Wait()
	bucket.NumberOfObjects = numObjects
	bucket.Objects = objectArray
	return nil
}

func iterateBuckets(i int, bucket *s3.Bucket, sess *session.Session, bucketArray []model.MetaBucket, wg *sync.WaitGroup) {
	defer wg.Done()
	buck := model.MetaBucket{}
	buck.CreationDate = *bucket.CreationDate
	buck.Name = *bucket.Name
	svc := s3.New(sess)
	loc, err := svc.GetBucketLocation(&s3.GetBucketLocationInput{Bucket: bucket.Name})
	if err != nil {
		log.Errorf("unable to get bucket location. failed with error: %v\n", err)
	} else {
		buck.Region = *loc.LocationConstraint
	}
	newSvc := s3.New(sess, aws.NewConfig().WithRegion(buck.Region))
	tags, err := newSvc.GetBucketTagging(&s3.GetBucketTaggingInput{Bucket: bucket.Name})

	if err != nil && !strings.Contains(err.Error(), "NoSuchTagSet") {
		log.Errorf("unable to get bucket tags. failed with error: %v\n", err)
	} else {
		tagset := make(map[string]string)
		for _, tag := range tags.TagSet {
			tagset[*tag.Key] = *tag.Value
		}
		buck.BucketTags = tagset
	}
	err = ObjectList(sess, &buck)
	if err != nil {
		log.Errorf("error while collecting object metadata for bucket %v. failed with error: %v\n", buck.Name, err)
	}
	bucketArray[i] = buck
}

func BucketList(sess *session.Session) ([]model.MetaBucket, error) {
	svc := s3.New(sess)

	output, err := svc.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		log.Errorf("unable to list buckets. failed with error: %v\n", err)
		return nil, err
	}
	numBuckets := len(output.Buckets)
	bucketArray := make([]model.MetaBucket, numBuckets)
	wg := sync.WaitGroup{}
	for i, bucket := range output.Buckets {
		wg.Add(1)
		go iterateBuckets(i, bucket, sess, bucketArray, &wg)
	}
	wg.Wait()
	return bucketArray, err
}

func (ad *AwsAdapter) SyncMetadata(ctx context.Context, in *pb.SyncMetadataRequest) error {

	buckArr, err := BucketList(ad.Session)
	if err != nil {
		log.Errorf("metadata collection failed with error: %v\n", err)
		return err
	}
	db.DbAdapter.CreateMetadata(ctx, ad.Backend, buckArr)
	return nil
}

func (ad *AwsAdapter) DownloadObject() {
	log.Info("Implement me")
}
