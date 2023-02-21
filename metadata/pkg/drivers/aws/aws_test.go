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
	"errors"
	"testing"
	"time"

	"github.com/opensds/multi-cloud/metadata/pkg/model"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/stretchr/testify/assert"
)

func MockObjectList(svc s3iface.S3API, bucketName string) ([]*model.MetaObject, int64, error) {
	if bucketName == "ppp" {
		return nil, 0, errors.New("ListObjectsV2Error")
	} else if bucketName == "vui" {
		objectArray := []*model.MetaObject{
			{
				ObjectName:       "object1",
				LastModifiedDate: &TIMEVAL,
				Size:             415,
				StorageClass:     "STANDARD",
				RedirectLocation: "/something",
				Metadata:         map[string]string{},
			},
			{
				ObjectName:       "object1",
				LastModifiedDate: &TIMEVAL,
				Size:             415,
				StorageClass:     "STANDARD",
				RedirectLocation: "/something",
				Metadata:         map[string]string{},
			},
		}
		return objectArray, 830, nil
	}
	return []*model.MetaObject{}, 0, nil
}

type MockS3Client struct {
	s3iface.S3API
	ListBucketsResp        *s3.ListBucketsOutput
	ListBucketsError       error
	GetBucketLocationResp  *s3.GetBucketLocationOutput
	GetBucketLocationError error
	GetBucketTaggingResp   *s3.GetBucketTaggingOutput
	GetBucketTaggingError  error
	GetBucketAclResp       *s3.GetBucketAclOutput
	GetBucketAclError      error
	ListObjectsV2Resp      *s3.ListObjectsV2Output
	ListObjectsV2Error     error
	GetObjectTaggingResp   *s3.GetObjectTaggingOutput
	GetObjectTaggingError  error
	GetObjectAclResp       *s3.GetObjectAclOutput
	GetObjectAclError      error
	HeadObjectResp         *s3.HeadObjectOutput
	HeadObjectError        error
	BucketName             string
}

type Test struct {
	TestNumber             int
	ListBucketsResp        *s3.ListBucketsOutput
	ListBucketsError       error
	GetBucketLocationResp  *s3.GetBucketLocationOutput
	GetBucketLocationError error
	GetBucketTaggingResp   *s3.GetBucketTaggingOutput
	GetBucketTaggingError  error
	GetBucketAclResp       *s3.GetBucketAclOutput
	GetBucketAclError      error
	ListObjectsV2Resp      *s3.ListObjectsV2Output
	ListObjectsV2Error     error
	GetObjectTaggingResp   *s3.GetObjectTaggingOutput
	GetObjectTaggingError  error
	GetObjectAclResp       *s3.GetObjectAclOutput
	GetObjectAclError      error
	HeadObjectResp         *s3.HeadObjectOutput
	HeadObjectError        error
	ExpectedBucketResp     []*model.MetaBucket
	ExpectedObjectResp     []*model.MetaObject
	ExpectedError          error
	BucketName             string
	ExpectedBucketSize     int64
}

var TIMEVAL = time.Date(2023, time.February, 12, 9, 23, 34, 45, time.UTC)

func TestObjectList(t *testing.T) {
	tests := []Test{
		{
			TestNumber: 1,
			BucketName: "abc",
			ListObjectsV2Resp: &s3.ListObjectsV2Output{Contents: []*s3.Object{
				{Key: aws.String("object1"), LastModified: &TIMEVAL, Size: aws.Int64(415), StorageClass: aws.String("STANDARD")},
				{Key: aws.String("object2"), LastModified: &TIMEVAL, Size: aws.Int64(425), StorageClass: aws.String("GLACIER_IR")},
				{Key: aws.String("object3"), LastModified: &TIMEVAL, Size: aws.Int64(435), StorageClass: aws.String("STANDARD")},
			}},
			HeadObjectError:       errors.New("HeadObjectError"),
			GetObjectAclError:     errors.New("GetObjectAclError"),
			GetObjectTaggingError: errors.New("GetObjectTaggingError"),

			ExpectedObjectResp: []*model.MetaObject{
				{
					ObjectName:       "object1",
					LastModifiedDate: &TIMEVAL,
					Size:             415,
					StorageClass:     "STANDARD",
				},
				{
					ObjectName:       "object2",
					LastModifiedDate: &TIMEVAL,
					Size:             425,
					StorageClass:     "GLACIER_IR",
				},
				{
					ObjectName:       "object3",
					LastModifiedDate: &TIMEVAL,
					Size:             435,
					StorageClass:     "STANDARD",
				},
			},
			ExpectedBucketSize: 1275,
			ExpectedError:      nil,
		},

		{
			TestNumber: 2,
			BucketName: "abc",
			ListObjectsV2Resp: &s3.ListObjectsV2Output{Contents: []*s3.Object{
				{Key: aws.String("object1"), LastModified: &TIMEVAL, Size: aws.Int64(1533), StorageClass: aws.String("STANDARD")},
			}},
			HeadObjectError:       errors.New("HeadObjectError"),
			GetObjectAclError:     errors.New("GetObjectAclError"),
			GetObjectTaggingError: errors.New("GetObjectTaggingError"),

			ExpectedObjectResp: []*model.MetaObject{
				{
					ObjectName:       "object1",
					LastModifiedDate: &TIMEVAL,
					Size:             1533,
					StorageClass:     "STANDARD",
				},
			},
			ExpectedBucketSize: 1533,
			ExpectedError:      nil,
		},

		{
			TestNumber:            3,
			BucketName:            "abc",
			ListObjectsV2Resp:     &s3.ListObjectsV2Output{},
			HeadObjectError:       errors.New("HeadObjectError"),
			GetObjectAclError:     errors.New("GetObjectAclError"),
			GetObjectTaggingError: errors.New("GetObjectTaggingError"),

			ExpectedObjectResp: []*model.MetaObject{},
			ExpectedBucketSize: 0,
			ExpectedError:      nil,
		},

		{
			TestNumber: 4,
			BucketName: "ghi",
			ListObjectsV2Resp: &s3.ListObjectsV2Output{Contents: []*s3.Object{
				{Key: aws.String("object1"), LastModified: &TIMEVAL, Size: aws.Int64(415), StorageClass: aws.String("STANDARD")},
				{Key: aws.String("object1"), LastModified: &TIMEVAL, Size: aws.Int64(415), StorageClass: aws.String("STANDARD")},
			}},
			HeadObjectResp: &s3.HeadObjectOutput{
				ServerSideEncryption:    aws.String("AE256"),
				ContentType:             aws.String("msword"),
				Expires:                 aws.String("2023-02-12T09:23:34.000000045Z"),
				ReplicationStatus:       aws.String("confirmed"),
				WebsiteRedirectLocation: aws.String("/something"),
				Metadata: map[string]*string{
					"k1": aws.String("v1"),
					"k2": aws.String("v2"),
				}},
			GetObjectAclError:     errors.New("GetObjectAclError"),
			GetObjectTaggingError: errors.New("GetObjectTaggingError"),

			ExpectedObjectResp: []*model.MetaObject{
				{
					ObjectName:           "object1",
					LastModifiedDate:     &TIMEVAL,
					Size:                 415,
					StorageClass:         "STANDARD",
					ServerSideEncryption: "AE256",
					ObjectType:           "msword",
					ExpiresDate:          &TIMEVAL,
					ReplicationStatus:    "confirmed",
					RedirectLocation:     "/something",
					Metadata:             map[string]string{"k1": "v1", "k2": "v2"},
				},
				{
					ObjectName:           "object1",
					LastModifiedDate:     &TIMEVAL,
					Size:                 415,
					StorageClass:         "STANDARD",
					ServerSideEncryption: "AE256",
					ObjectType:           "msword",
					ExpiresDate:          &TIMEVAL,
					ReplicationStatus:    "confirmed",
					RedirectLocation:     "/something",
					Metadata:             map[string]string{"k1": "v1", "k2": "v2"},
				},
			},
			ExpectedBucketSize: 830,
			ExpectedError:      nil,
		},

		{
			TestNumber: 5,
			BucketName: "vui",
			ListObjectsV2Resp: &s3.ListObjectsV2Output{Contents: []*s3.Object{
				{Key: aws.String("object1"), LastModified: &TIMEVAL, Size: aws.Int64(415), StorageClass: aws.String("STANDARD")},
				{Key: aws.String("object1"), LastModified: &TIMEVAL, Size: aws.Int64(415), StorageClass: aws.String("STANDARD")},
			}},
			HeadObjectResp: &s3.HeadObjectOutput{
				Expires:                 aws.String("2022-11-.000Z"),
				WebsiteRedirectLocation: aws.String("/something"),
			},
			GetObjectAclError:     errors.New("GetObjectAclError"),
			GetObjectTaggingError: errors.New("GetObjectTaggingError"),

			ExpectedObjectResp: []*model.MetaObject{
				{
					ObjectName:       "object1",
					LastModifiedDate: &TIMEVAL,
					Size:             415,
					StorageClass:     "STANDARD",
					RedirectLocation: "/something",
					Metadata:         map[string]string{},
				},
				{
					ObjectName:       "object1",
					LastModifiedDate: &TIMEVAL,
					Size:             415,
					StorageClass:     "STANDARD",
					RedirectLocation: "/something",
					Metadata:         map[string]string{},
				},
			},
			ExpectedBucketSize: 830,
			ExpectedError:      nil,
		},

		{
			TestNumber:         6,
			BucketName:         "abc",
			ListObjectsV2Error: errors.New("ListObjectsForbidden"),

			ExpectedObjectResp: nil,
			ExpectedError:      errors.New("ListObjectsForbidden"),
		},

		{
			TestNumber: 7,
			BucketName: "xyz",
			ListObjectsV2Resp: &s3.ListObjectsV2Output{Contents: []*s3.Object{
				{Key: aws.String("object1"), LastModified: &TIMEVAL, Size: aws.Int64(415), StorageClass: aws.String("STANDARD")},
				{Key: aws.String("object1"), LastModified: &TIMEVAL, Size: aws.Int64(415), StorageClass: aws.String("STANDARD")},
			}},
			HeadObjectError:      errors.New("HeadObjectError"),
			GetObjectTaggingResp: &s3.GetObjectTaggingOutput{TagSet: []*s3.Tag{{Key: aws.String("key1"), Value: aws.String("val1")}}, VersionId: aws.String("dubwvnkewv.fewvbib")},
			GetObjectAclError:    errors.New("GetObjectAclError"),

			ExpectedObjectResp: []*model.MetaObject{
				{
					ObjectName:       "object1",
					LastModifiedDate: &TIMEVAL,
					Size:             415,
					StorageClass:     "STANDARD",
					ObjectTags:       map[string]string{"key1": "val1"},
					VersionId:        "dubwvnkewv.fewvbib",
				},
				{
					ObjectName:       "object1",
					LastModifiedDate: &TIMEVAL,
					Size:             415,
					StorageClass:     "STANDARD",
					ObjectTags:       map[string]string{"key1": "val1"},
					VersionId:        "dubwvnkewv.fewvbib",
				},
			},
			ExpectedBucketSize: 830,
			ExpectedError:      nil,
		},

		{
			TestNumber: 8,
			ListObjectsV2Resp: &s3.ListObjectsV2Output{Contents: []*s3.Object{
				{Key: aws.String("object1"), LastModified: &TIMEVAL, Size: aws.Int64(415), StorageClass: aws.String("STANDARD")},
			}},
			HeadObjectError:       errors.New("HeadObjectError"),
			GetObjectTaggingError: errors.New("GetObjectTaggingError"),
			GetObjectAclResp:      &s3.GetObjectAclOutput{Grants: []*s3.Grant{{Grantee: &s3.Grantee{ID: aws.String("ewwiubibu")}, Permission: aws.String("FULL_CONTROL")}}},

			ExpectedObjectResp: []*model.MetaObject{
				{
					ObjectName:       "object1",
					LastModifiedDate: &TIMEVAL,
					Size:             415,
					StorageClass:     "STANDARD",
					ObjectAcl:        []*model.Access{{ID: "ewwiubibu", Permission: "FULL_CONTROL"}},
				},
			},
			ExpectedBucketSize: 415,
			ExpectedError:      nil,
		},

		{
			TestNumber: 9,
			BucketName: "ppp",
			ListObjectsV2Resp: &s3.ListObjectsV2Output{Contents: []*s3.Object{
				{Key: aws.String("object1"), LastModified: &TIMEVAL, Size: aws.Int64(415), StorageClass: aws.String("STANDARD")},
			}},
			HeadObjectError:       errors.New("HeadObjectError"),
			GetObjectAclError:     errors.New("GetObjectAclError"),
			GetObjectTaggingError: errors.New("GetObjectTaggingError"),

			ExpectedObjectResp: nil,
			ExpectedBucketSize: 0,
			ExpectedError:      errors.New("ListObjectsV2Error"),
		},
	}
	for _, test := range tests {
		mockSvc := &MockS3Client{
			HeadObjectResp:        test.HeadObjectResp,
			HeadObjectError:       test.HeadObjectError,
			GetObjectAclResp:      test.GetObjectAclResp,
			GetObjectAclError:     test.GetObjectAclError,
			ListObjectsV2Resp:     test.ListObjectsV2Resp,
			ListObjectsV2Error:    test.ListObjectsV2Error,
			GetObjectTaggingResp:  test.GetObjectTaggingResp,
			GetObjectTaggingError: test.GetObjectTaggingError,
			BucketName:            test.BucketName,
		}
		objectArray, totalSize, err := ObjectList(mockSvc, mockSvc.BucketName)
		assert.Equal(t, test.ExpectedObjectResp, objectArray, test.TestNumber)
		assert.Equal(t, test.ExpectedBucketSize, totalSize, test.TestNumber)
		assert.Equal(t, test.ExpectedError, err, test.TestNumber)
	}
}

func TestBucketList(t *testing.T) {
	tests := []Test{
		{
			TestNumber: 1,
			ListBucketsResp: &s3.ListBucketsOutput{Buckets: []*s3.Bucket{
				{CreationDate: &TIMEVAL, Name: aws.String("abc")},
			},
			},
			GetBucketLocationResp: &s3.GetBucketLocationOutput{LocationConstraint: aws.String("ap-south-1")},
			GetBucketTaggingResp: &s3.GetBucketTaggingOutput{TagSet: []*s3.Tag{
				{Key: aws.String("test1"), Value: aws.String("test2")},
				{Key: aws.String("sample1"), Value: aws.String("sample2")},
			}},
			GetBucketAclError: errors.New("GetBucketAclError"),

			ExpectedBucketResp: []*model.MetaBucket{
				{
					CreationDate:    &TIMEVAL,
					Name:            "abc",
					Region:          "ap-south-1",
					NumberOfObjects: 0,
					TotalSize:       0,
					BucketTags:      map[string]string{"test1": "test2", "sample1": "sample2"},
					Objects:         []*model.MetaObject{},
				},
			},
			ExpectedError: nil,
		},

		{
			TestNumber:       2,
			ListBucketsError: errors.New("ListBucketsForbidden"),

			ExpectedBucketResp: nil,
			ExpectedError:      errors.New("ListBucketsForbidden"),
		},

		{
			ListBucketsResp: &s3.ListBucketsOutput{Buckets: []*s3.Bucket{
				{CreationDate: &TIMEVAL, Name: aws.String("abc")},
				{CreationDate: &TIMEVAL, Name: aws.String("ghi")},
			},
			},
			GetBucketLocationError: errors.New("GetBucketLocationError"),

			ExpectedBucketResp: []*model.MetaBucket{},
			ExpectedError:      nil,
		},

		{
			TestNumber: 3,
			ListBucketsResp: &s3.ListBucketsOutput{Buckets: []*s3.Bucket{
				{CreationDate: &TIMEVAL, Name: aws.String("abc")},
			},
			},
			GetBucketLocationResp: &s3.GetBucketLocationOutput{LocationConstraint: aws.String("ap-south-1")},
			GetBucketTaggingError: errors.New("GetBucketTaggingError"),
			GetBucketAclError:     errors.New("GetBucketAclError"),

			ExpectedBucketResp: []*model.MetaBucket{
				{
					CreationDate:    &TIMEVAL,
					Name:            "abc",
					Region:          "ap-south-1",
					NumberOfObjects: 0,
					TotalSize:       0,
					Objects:         []*model.MetaObject{},
				},
			},
			ExpectedError: nil,
		},

		{
			TestNumber: 4,
			ListBucketsResp: &s3.ListBucketsOutput{Buckets: []*s3.Bucket{
				{CreationDate: &TIMEVAL, Name: aws.String("abc")},
			},
			},
			GetBucketLocationResp: &s3.GetBucketLocationOutput{LocationConstraint: aws.String("ap-south-1")},
			GetBucketTaggingResp:  &s3.GetBucketTaggingOutput{},
			GetBucketTaggingError: errors.New("NoSuchTagSet"),
			GetBucketAclError:     errors.New("GetBucketAclError"),

			ExpectedBucketResp: []*model.MetaBucket{
				{
					CreationDate:    &TIMEVAL,
					Name:            "abc",
					Region:          "ap-south-1",
					NumberOfObjects: 0,
					TotalSize:       0,
					BucketTags:      map[string]string{},
					Objects:         []*model.MetaObject{},
				},
			},
			ExpectedError: nil,
		},

		{
			TestNumber: 5,
			ListBucketsResp: &s3.ListBucketsOutput{Buckets: []*s3.Bucket{
				{CreationDate: &TIMEVAL, Name: aws.String("abc")},
			},
			},
			GetBucketLocationResp: &s3.GetBucketLocationOutput{LocationConstraint: aws.String("ap-south-1")},
			GetBucketTaggingError: errors.New("GetBucketTaggingError"),
			GetBucketAclError:     errors.New("GetBucketAclError"),

			ExpectedBucketResp: []*model.MetaBucket{
				{
					CreationDate:    &TIMEVAL,
					Name:            "abc",
					Region:          "ap-south-1",
					NumberOfObjects: 0,
					TotalSize:       0,
					Objects:         []*model.MetaObject{},
				},
			},
			ExpectedError: nil,
		},

		{
			TestNumber: 6,
			ListBucketsResp: &s3.ListBucketsOutput{Buckets: []*s3.Bucket{
				{CreationDate: &TIMEVAL, Name: aws.String("abc")},
				{CreationDate: &TIMEVAL, Name: aws.String("ghi")},
			},
			},
			GetBucketLocationResp: &s3.GetBucketLocationOutput{LocationConstraint: aws.String("ap-northeast-1")},

			ExpectedBucketResp: []*model.MetaBucket{},
			ExpectedError:      nil,
		},

		{
			TestNumber: 7,
			ListBucketsResp: &s3.ListBucketsOutput{Buckets: []*s3.Bucket{
				{CreationDate: &TIMEVAL, Name: aws.String("ghi")},
			},
			},
			GetBucketLocationResp: &s3.GetBucketLocationOutput{LocationConstraint: aws.String("ap-south-1")},
			GetBucketTaggingError: errors.New("GetBucketTaggingError"),
			GetBucketAclError:     errors.New("GetBucketAclError"),

			ExpectedBucketResp: []*model.MetaBucket{
				{
					CreationDate:    &TIMEVAL,
					Name:            "ghi",
					Region:          "ap-south-1",
					NumberOfObjects: 0,
					TotalSize:       0,
					Objects:         []*model.MetaObject{},
				},
			},
			ExpectedError: nil,
		},

		{
			TestNumber: 8,
			ListBucketsResp: &s3.ListBucketsOutput{Buckets: []*s3.Bucket{
				{CreationDate: &TIMEVAL, Name: aws.String("vui")},
			},
			},
			GetBucketLocationResp: &s3.GetBucketLocationOutput{LocationConstraint: aws.String("ap-south-1")},
			GetBucketTaggingError: errors.New("GetBucketTaggingError"),
			GetBucketAclError:     errors.New("GetBucketAclError"),

			ExpectedBucketResp: []*model.MetaBucket{
				{
					CreationDate:    &TIMEVAL,
					Name:            "vui",
					Region:          "ap-south-1",
					NumberOfObjects: 2,
					TotalSize:       830,
					Objects: []*model.MetaObject{
						{
							ObjectName:       "object1",
							LastModifiedDate: &TIMEVAL,
							Size:             415,
							StorageClass:     "STANDARD",
							RedirectLocation: "/something",
							Metadata:         map[string]string{},
						},
						{
							ObjectName:       "object1",
							LastModifiedDate: &TIMEVAL,
							Size:             415,
							StorageClass:     "STANDARD",
							RedirectLocation: "/something",
							Metadata:         map[string]string{},
						},
					},
				},
			},
			ExpectedError: nil,
		},

		{
			TestNumber: 9,
			ListBucketsResp: &s3.ListBucketsOutput{Buckets: []*s3.Bucket{
				{CreationDate: &TIMEVAL, Name: aws.String("abc")},
			},
			},
			GetBucketLocationResp: &s3.GetBucketLocationOutput{LocationConstraint: aws.String("ap-south-1")},
			GetBucketTaggingError: errors.New("GetBucketTaggingError"),
			GetBucketAclResp:      &s3.GetBucketAclOutput{Grants: []*s3.Grant{{Grantee: &s3.Grantee{ID: aws.String("ewwiubibu")}, Permission: aws.String("FULL_CONTROL")}}},

			ExpectedBucketResp: []*model.MetaBucket{
				{
					CreationDate:    &TIMEVAL,
					Name:            "abc",
					Region:          "ap-south-1",
					NumberOfObjects: 0,
					TotalSize:       0,
					BucketAcl:       []*model.Access{{ID: "ewwiubibu", Permission: "FULL_CONTROL"}},
					Objects:         []*model.MetaObject{},
				},
			},
			ExpectedError: nil,
		},

		{
			TestNumber: 10,
			ListBucketsResp: &s3.ListBucketsOutput{Buckets: []*s3.Bucket{
				{CreationDate: &TIMEVAL, Name: aws.String("abc")},
				{CreationDate: &TIMEVAL, Name: aws.String("qqq")},
				{CreationDate: &TIMEVAL, Name: aws.String("ghi")},
			},
			},
			GetBucketLocationResp: &s3.GetBucketLocationOutput{LocationConstraint: aws.String("ap-south-1")},
			GetBucketTaggingError: errors.New("GetBucketTaggingError"),
			GetBucketAclError:     errors.New("GetBucketAclError"),

			ExpectedBucketResp: []*model.MetaBucket{
				{
					CreationDate:    &TIMEVAL,
					Name:            "abc",
					Region:          "ap-south-1",
					NumberOfObjects: 0,
					TotalSize:       0,
					Objects:         []*model.MetaObject{},
				},
				{
					CreationDate:    &TIMEVAL,
					Name:            "ghi",
					Region:          "ap-south-1",
					NumberOfObjects: 0,
					TotalSize:       0,
					Objects:         []*model.MetaObject{},
				},
			},
			ExpectedError: nil,
		},

		{
			TestNumber: 11,
			ListBucketsResp: &s3.ListBucketsOutput{Buckets: []*s3.Bucket{
				{CreationDate: &TIMEVAL, Name: aws.String("abc")},
				{CreationDate: &TIMEVAL, Name: aws.String("qqq")},
				{CreationDate: &TIMEVAL, Name: aws.String("ppp")},
			},
			},
			GetBucketLocationResp: &s3.GetBucketLocationOutput{LocationConstraint: aws.String("ap-south-1")},
			GetBucketTaggingError: errors.New("GetBucketTaggingError"),
			GetBucketAclError:     errors.New("GetBucketAclError"),

			ExpectedBucketResp: []*model.MetaBucket{
				{
					CreationDate:    &TIMEVAL,
					Name:            "abc",
					Region:          "ap-south-1",
					NumberOfObjects: 0,
					TotalSize:       0,
					Objects:         []*model.MetaObject{},
				},
			},
			ExpectedError: nil,
		},
	}

	for _, test := range tests {
		backendRegion := "ap-south-1"
		mockSvc := &MockS3Client{
			ListBucketsResp:        test.ListBucketsResp,
			ListBucketsError:       test.ListBucketsError,
			GetBucketLocationResp:  test.GetBucketLocationResp,
			GetBucketLocationError: test.GetBucketLocationError,
			HeadObjectResp:         test.HeadObjectResp,
			HeadObjectError:        test.HeadObjectError,
			GetObjectAclResp:       test.GetObjectAclResp,
			GetObjectAclError:      test.GetObjectAclError,
			GetBucketTaggingResp:   test.GetBucketTaggingResp,
			GetBucketTaggingError:  test.GetBucketTaggingError,
			ListObjectsV2Resp:      test.ListObjectsV2Resp,
			ListObjectsV2Error:     test.ListObjectsV2Error,
			GetBucketAclResp:       test.GetBucketAclResp,
			GetBucketAclError:      test.GetBucketAclError,
			GetObjectTaggingResp:   test.GetObjectTaggingResp,
			GetObjectTaggingError:  test.GetObjectTaggingError,
		}
		ObjectList = MockObjectList
		bucketArray, err := BucketList(mockSvc, &backendRegion)

		assert.Equal(t, test.ExpectedBucketResp, bucketArray, test.TestNumber)
		assert.Equal(t, test.ExpectedError, err, test.TestNumber)
	}
}

func (m *MockS3Client) ListBuckets(input *s3.ListBucketsInput) (*s3.ListBucketsOutput, error) {
	return m.ListBucketsResp, m.ListBucketsError
}

func (m *MockS3Client) GetBucketLocation(input *s3.GetBucketLocationInput) (*s3.GetBucketLocationOutput, error) {
	if *input.Bucket == "qqq" {
		return &s3.GetBucketLocationOutput{LocationConstraint: aws.String("ap-northeast-1")}, m.GetBucketLocationError
	}
	return m.GetBucketLocationResp, m.GetBucketLocationError
}

func (m *MockS3Client) GetBucketTagging(input *s3.GetBucketTaggingInput) (*s3.GetBucketTaggingOutput, error) {
	return m.GetBucketTaggingResp, m.GetBucketTaggingError
}

func (m *MockS3Client) GetBucketAcl(input *s3.GetBucketAclInput) (*s3.GetBucketAclOutput, error) {
	return m.GetBucketAclResp, m.GetBucketAclError
}

func (m *MockS3Client) ListObjectsV2(input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	if *input.Bucket == "ppp" {
		return nil, errors.New("ListObjectsV2Error")
	}
	return m.ListObjectsV2Resp, m.ListObjectsV2Error
}

func (m *MockS3Client) GetObjectTagging(*s3.GetObjectTaggingInput) (*s3.GetObjectTaggingOutput, error) {
	return m.GetObjectTaggingResp, m.GetObjectTaggingError
}

func (m *MockS3Client) GetObjectAcl(*s3.GetObjectAclInput) (*s3.GetObjectAclOutput, error) {
	return m.GetObjectAclResp, m.GetObjectAclError
}

func (m *MockS3Client) HeadObject(*s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	return m.HeadObjectResp, m.HeadObjectError
}
