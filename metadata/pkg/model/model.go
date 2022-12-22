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

	"github.com/globalsign/mgo/bson"
)

type MetaBucket struct {
	Id           bson.ObjectId `json:"id,omitempty" bson:"_id,omitempty"`
	CreationDate *time.Time    `type:"timestamp"`
	Name         *string       `type:"string"`
	Type         string        `json:"type,omitempty" bson:"type,omitempty"`
	Region       string        `json:"region,omitempty" bson:"region,omitempty"`
	Tags         string        `json:"tags,omitempty" bson:"tags,omitempty"`
}
