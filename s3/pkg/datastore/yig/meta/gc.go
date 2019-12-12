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
package meta

import (
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/meta/types"
)

// delete multipart uploaded part objects and put them into gc
func (m *Meta) PutPartsInGc(parts []*types.PartInfo) error {
	return m.db.PutPartsInGc(parts)
}

func (m *Meta) PutGcObjects(objects ...*types.GcObject) error {
	return m.db.PutGcObjects(objects...)
}

// get gc objects by marker and limit
func (m *Meta) GetGcObjects(marker int64, limit int) ([]*types.GcObject, error) {
	return m.db.GetGcObjects(marker, limit)
}

// delete gc objects meta.
func (m *Meta) DeleteGcObjects(objects ...*types.GcObject) error {
	return m.db.DeleteGcObjects(objects...)
}
