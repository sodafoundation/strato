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
	"context"

	. "github.com/opensds/multi-cloud/s3/pkg/meta/types"
)

func (m *Meta) GetMultipart(bucketName, objectName, uploadId string) (Multipart, error) {
	return m.Db.GetMultipart(bucketName, objectName, uploadId)
}

func (m *Meta) DeleteMultipart(ctx context.Context, multipart Multipart) (err error) {
	tx, err := m.Db.NewTrans()
	defer func() {
		if err != nil {
			m.Db.AbortTrans(tx)
		}
	}()
	err = m.Db.DeleteMultipart(&multipart, tx)
	if err != nil {
		return
	}

	// TODO: usage need to be updated for charging, it depends on redis, and the mechanism is:
	// 1. Update usage in redis when each delete happens.
	// 2. Update usage in database periodically based on redis.
	// see https://github.com/opensds/multi-cloud/issues/698 for redis related issue.

	err = m.Db.CommitTrans(tx)
	return
}
