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
	_ "github.com/mailru/easyjson/gen"
	"time"
)

//easyjson:json
type MetaBackend struct {
	Id                      string        `json:"id" bson:"_id"`
	BackendName             string        `json:"backendName" bson:"backendName"`
	Type                    string        `json:"type" bson:"type"`
	Region                  string        `json:"region" bson:"region"`
	Buckets                 []*MetaBucket `json:"buckets" bson:"buckets"`
	NumberOfBuckets         int32         `json:"numberOfBuckets" bson:"numberOfBuckets"`
	NumberOfFilteredBuckets int32         `json:"numberOFilteredBuckets" bson:"numberOFilteredBuckets"`
}

//easyjson:json
type MetaBucket struct {
	CreationDate            *time.Time        `type:"timestamp" json:"creationDate,omitempty" bson:"creationDate,omitempty"`
	Name                    string            `type:"string" json:"name" bson:"name"`
	Type                    string            `json:"type,omitempty" bson:"type,omitempty"`
	Region                  string            `json:"region,omitempty" bson:"region,omitempty"`
	Access                  string            `json:"access,omitempty" bson:"access,omitempty"`
	NumberOfObjects         int               `json:"numberOfObjects" bson:"numberOfObjects,omitempty"`
	NumberOfFilteredObjects int               `json:"numberOfFilteredObjects,omitempty" bson:"numberOfFilteredObjects,omitempty"`
	Objects                 []*MetaObject     `json:"objects" bson:"objects"`
	TotalSize               int64             `json:"totalSize" bson:"totalSize"`
	FilteredBucketSize      int64             `json:"filteredBucketSize,omitempty" bson:"filteredBucketSize"`
	BucketTags              map[string]string `json:"tags,omitempty" bson:"tags,omitempty"`
}

//easyjson:json
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
	GrantControl         string            `json:"grantControl,omitempty" bson:"grantControl,omitempty"`
	ObjectTags           map[string]string `json:"objectTags,omitempty" bson:"objectTags,omitempty"`
	Metadata             map[string]string `json:"metadata,omitempty" bson:"metadata,omitempty"`
	ObjectType           string            `json:"objectType,omitempty" bson:"objectType,omitempty"`
}

//easyjson:json
type ListMetaDataResponse []*MetaBackend
