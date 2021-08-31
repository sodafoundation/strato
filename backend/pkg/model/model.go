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
	"github.com/globalsign/mgo/bson"
)

type Backend struct {
	Id         bson.ObjectId `json:"id,omitempty" bson:"_id,omitempty"`
	TenantId   string        `json:"tenantId,omitempty" bson:"tenantId,omitempty"`
	UserId     string        `json:"userId,omitempty" bson:"userId,omitempty"`
	Name       string        `json:"name,omitempty" bson:"name,omitempty"`
	Type       string        `json:"type,omitempty" bson:"type,omitempty"`
	Region     string        `json:"region,omitempty" bson:"region,omitempty"`
	Endpoint   string        `json:"endpoint,omitempty" bson:"endpoint,omitempty"`
	BucketName string        `json:"bucketName,omitempty" bson:"bucketName,omitempty"`
	Access     string        `json:"access,omitempty" bson:"access,omitempty"`
	Security   string        `json:"security,omitempty" bson:"security,omitempty"`
}

type Ssp struct {
	Id       bson.ObjectId `json:"id,omitempty" bson:"_id,omitempty"`
	TenantId string        `json:"tenantId,omitempty" bson:"tenantId,omitempty"`
	Name     string        `json:"name,omitempty" bson:"name,omitempty"`
	Backends []string      `json:"type,omitempty" bson:"backends,omitempty"`
	Tenants  []string      `json:"type,omitempty" bson:"tenants,omitempty"`
}

