// Copyright 2020 The SODA Authors.
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

type Volume struct {
    Id bson.ObjectId `json:"id,omitempty" bson:"_id,omitempty"`
    CreatedAt string `json:"createdAt,omitempty" bson:"createdAt,omitempty"`
    UpdatedAt string `json:"updatedAt,omitempty" bson:"updatedAt,omitempty"`
    TenantId string `json:"tenantId,omitempty" bson:"tenantId,omitempty"`
    UserId string `json:"userId,omitempty" bson:"userId,omitempty"`
    BackendId string `json:"backendId,omitempty" bson:"backendId,omitempty"`
    Name string `json:"name,omitempty" bson:"name,omitempty"`
    Type string `json:"type,omitempty" bson:"type,omitempty"`
    AvailabilityZone string `json:"availabilityZone,omitempty" bson:"availabilityZone,omitempty"`
    State string `json:"status,omitempty" bson:"status,omitempty"`
    Size  int64 `json:"size,omitempty" bson:"size,omitempty"`
    Region string `json:"region,omitempty" bson:"region,omitempty"`
    VolumeId string `json:"region,omitempty" bson:"region,omitempty"`
    Encrypted bool `json:"encrypted,omitempty" bson:"encrypted,omitempty"`
    Iops  int64 `json:"size,omitempty" bson:"size,omitempty"`
}
