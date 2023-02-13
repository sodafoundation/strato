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

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	"github.com/opensds/multi-cloud/metadata/pkg/db"
	"github.com/opensds/multi-cloud/metadata/pkg/model"
	"github.com/opensds/multi-cloud/metadata/pkg/utils"
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

func GetHeadObject(sess *session.Session, bucketName *string, obj *model.MetaObject) {
	svc := s3.New(sess)
	meta, err := svc.HeadObject(&s3.HeadObjectInput{Bucket: bucketName, Key: &obj.ObjectName})
	if err != nil {
		log.Errorf("cannot perform head object on object %v in bucket %v. failed with error: %v", obj.ObjectName, *bucketName, err)
		return
	}
	if meta.ServerSideEncryption != nil {
		obj.ServerSideEncryption = *meta.ServerSideEncryption
	}
	obj.ObjectType = *meta.ContentType
	if meta.Expires != nil {
		expiresTime, err := time.Parse(time.RFC3339, *meta.Expires)
		if err != nil {
			log.Errorf("unable to parse given string to time type. error: %v. skipping ExpiresDate field", err)
		} else {
			obj.ExpiresDate = &expiresTime
		}
	}
	if meta.ReplicationStatus != nil {
		obj.ReplicationStatus = *meta.ReplicationStatus
	}
	if meta.WebsiteRedirectLocation != nil {
		obj.RedirectLocation = *meta.WebsiteRedirectLocation
	}
	metadata := map[string]string{}
	for key, val := range meta.Metadata {
		metadata[key] = *val
	}
	obj.Metadata = metadata
}

func ObjectList(sess *session.Session, bucket *model.MetaBucket) error {
	svc := s3.New(sess)
	output, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: &bucket.Name})
	if err != nil {
		log.Errorf("unable to list objects in bucket %v. failed with error: %v", bucket.Name, err)
		return err
	}

	numObjects := len(output.Contents)
	var totSize int64
	objectArray := make([]*model.MetaObject, numObjects)
	for objIdx, object := range output.Contents {
		obj := &model.MetaObject{}
		objectArray[objIdx] = obj
		obj.LastModifiedDate = object.LastModified
		obj.ObjectName = *object.Key
		obj.Size = *object.Size
		totSize += obj.Size
		obj.StorageClass = *object.StorageClass

		tags, err := svc.GetObjectTagging(&s3.GetObjectTaggingInput{Bucket: &bucket.Name, Key: &obj.ObjectName})

		if err == nil {
			tagset := map[string]string{}
			for _, tag := range tags.TagSet {
				tagset[*tag.Key] = *tag.Value
			}
			obj.ObjectTags = tagset
			if tags.VersionId != nil {
				obj.VersionId = *tags.VersionId
			}
		} else {
			log.Errorf("unable to get object tags. failed with error: %v", err)
		}

		acl, err := svc.GetObjectAcl(&s3.GetObjectAclInput{Bucket: &bucket.Name, Key: &obj.ObjectName})
		if err != nil {
			log.Errorf("unable to get object Acl. failed with error: %v", err)
		} else {
			access := []*model.Access{}
			for _, grant := range acl.Grants {
				access = append(access, utils.AclMapper(grant))
			}
			obj.ObjectAcl = access
		}

		GetHeadObject(sess, &bucket.Name, obj)
	}
	bucket.NumberOfObjects = numObjects
	bucket.TotalSize = totSize
	bucket.Objects = objectArray
	return nil
}

func GetBucketMeta(buckIdx int, bucket *s3.Bucket, sess *session.Session, bucketArray []*model.MetaBucket, wg *sync.WaitGroup) {
	defer wg.Done()

	svc := s3.New(sess)
	loc, err := svc.GetBucketLocation(&s3.GetBucketLocationInput{Bucket: bucket.Name})
	if err != nil {
		log.Errorf("unable to get bucket location. failed with error: %v", err)
		return
	}

	if *loc.LocationConstraint != *sess.Config.Region {
		return
	}

	buck := &model.MetaBucket{}
	buck.Name = *bucket.Name
	buck.CreationDate = bucket.CreationDate
	buck.Region = *loc.LocationConstraint

	err = ObjectList(sess, buck)
	if err != nil {
		return
	}

	bucketArray[buckIdx] = buck

	tags, err := svc.GetBucketTagging(&s3.GetBucketTaggingInput{Bucket: bucket.Name})

	if err == nil {
		tagset := map[string]string{}
		for _, tag := range tags.TagSet {
			tagset[*tag.Key] = *tag.Value
		}
		buck.BucketTags = tagset
	} else if !strings.Contains(err.Error(), "NoSuchTagSet") {
		log.Errorf("unable to get bucket tags. failed with error: %v", err)
	}

	acl, err := svc.GetBucketAcl(&s3.GetBucketAclInput{Bucket: bucket.Name})
	if err != nil {
		log.Errorf("unable to get bucket Acl. failed with error: %v", err)
	} else {
		access := []*model.Access{}
		for _, grant := range acl.Grants {
			access = append(access, utils.AclMapper(grant))
		}
		buck.BucketAcl = access
	}
}

func BucketList(sess *session.Session) ([]*model.MetaBucket, error) {
	svc := s3.New(sess)

	output, err := svc.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		log.Errorf("unable to list buckets. failed with error: %v", err)
		return nil, err
	}
	bucketArray := make([]*model.MetaBucket, len(output.Buckets))
	wg := sync.WaitGroup{}
	for idx, bucket := range output.Buckets {
		wg.Add(1)
		go GetBucketMeta(idx, bucket, sess, bucketArray, &wg)
	}
	wg.Wait()

	bucketArrayFiltered := []*model.MetaBucket{}
	for _, buck := range bucketArray {
		if buck != nil {
			bucketArrayFiltered = append(bucketArrayFiltered, buck)
		}
	}

	return bucketArrayFiltered, err
}

func (ad *AwsAdapter) SyncMetadata(ctx context.Context, in *pb.SyncMetadataRequest) error {

	buckArr, err := BucketList(ad.Session)
	if err != nil {
		log.Errorf("metadata collection for backend id: %v failed with error: %v", ad.Backend.Id, err)
		return err
	}

	metaBackend := model.MetaBackend{}
	metaBackend.Id = ad.Backend.Id
	metaBackend.BackendName = ad.Backend.Name
	metaBackend.Type = ad.Backend.Type
	metaBackend.Region = ad.Backend.Region
	metaBackend.Buckets = buckArr
	metaBackend.NumberOfBuckets = int32(len(buckArr))
	newContext := context.TODO()
	err = db.DbAdapter.CreateMetadata(newContext, metaBackend)

	return err
}

func (ad *AwsAdapter) DownloadObject() {
	log.Info("Implement me")
}
