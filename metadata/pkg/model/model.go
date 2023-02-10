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

package model

import (
	"time"
)

type MetaBackend struct {
	Id                      string        `json:"id" bson:"_id"`
	BackendName             string        `json:"backendName" bson:"backendName"`
	Type                    string        `json:"type" bson:"type"`
	Region                  string        `json:"region" bson:"region"`
	Buckets                 []*MetaBucket `json:"buckets" bson:"buckets"`
	NumberOfBuckets         int32         `json:"numberOfBuckets" bson:"numberOfBuckets"`
	NumberOfFilteredBuckets int32         `json:"numberOFilteredBuckets" bson:"numberOFilteredBuckets"`
}

type MetaBucket struct {
	CreationDate            *time.Time        `type:"timestamp" json:"creationDate,omitempty" bson:"creationDate,omitempty"`
	Name                    string            `type:"string" json:"name" bson:"name"`
	Type                    string            `json:"type,omitempty" bson:"type,omitempty"`
	Region                  string            `json:"region,omitempty" bson:"region,omitempty"`
	BucketAcl               []*Access         `json:"bucketAcl,omitempty" bson:"bucketAcl,omitempty"`
	NumberOfObjects         int               `json:"numberOfObjects" bson:"numberOfObjects"`
	NumberOfFilteredObjects int               `json:"numberOfFilteredObjects,omitempty" bson:"numberOfFilteredObjects,omitempty"`
	Objects                 []*MetaObject     `json:"objects" bson:"objects"`
	TotalSize               int64             `json:"totalSize" bson:"totalSize"`
	FilteredBucketSize      int64             `json:"filteredBucketSize,omitempty" bson:"filteredBucketSize"`
	BucketTags              map[string]string `json:"bucketTags,omitempty" bson:"bucketTags,omitempty"`
}

type MetaObject struct {
	ObjectName           string            `json:"name" bson:"name"`
	LastModifiedDate     *time.Time        `type:"timestamp" json:"lastModifiedDate" bson:"lastModifiedDate"`
	Size                 int64             `json:"size" bson:"size"`
	ServerSideEncryption string            `json:"serverSideEncryption" bson:"serverSideEncryption"`
	VersionId            string            `json:"versionId,omitempty" bson:"versionId,omitempty"`
	StorageClass         string            `json:"storageClass,omitempty" bson:"storageClass,omitempty"`
	RedirectLocation     string            `json:"redirectLocation,omitempty" bson:"redirectLocation,omitempty"`
	ReplicationStatus    string            `json:"replicationStatus,omitempty" bson:"replicationStatus,omitempty"`
	ExpiresDate          *time.Time        `json:"expiresDate,omitempty" bson:"expiresDate,omitempty"`
	ObjectAcl            []*Access         `json:"objectAcl,omitempty" bson:"objectAcl,omitempty"`
	ObjectTags           map[string]string `json:"objectTags,omitempty" bson:"objectTags,omitempty"`
	Metadata             map[string]string `json:"metadata,omitempty" bson:"metadata,omitempty"`
	ObjectType           string            `json:"objectType,omitempty" bson:"objectType,omitempty"`
}

type Access struct {
	DisplayName  string `json:"displayName,omitempty" bson:"displayName,omitempty"`
	EmailAddress string `json:"emailAddress,omitempty" bson:"emailAddress,omitempty"`
	ID           string `json:"id,omitempty" bson:"id,omitempty"`
	Type         string `json:"type,omitempty" bson:"type,omitempty"`
	URI          string `json:"uri,omitempty" bson:"uri,omitempty"`
	Permission   string `json:"permission,omitempty" bson:"permission,omitempty"`
}
