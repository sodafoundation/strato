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

package job

import (
	"context"
	log "github.com/sirupsen/logrus"
	"github.com/opensds/multi-cloud/dataflow/pkg/db"
	. "github.com/opensds/multi-cloud/dataflow/pkg/model"
)

func Create(ctx context.Context, job *Job) (*Job, error) {
	return db.DbAdapter.CreateJob(ctx, job)
}

func Get(ctx context.Context, id string) (*Job, error) {
	log.Infof("get job %s", id)
	return db.DbAdapter.GetJob(ctx, id)
}

func List(ctx context.Context, limit int, offset int, filter interface{}) ([]Job, error) {
	return db.DbAdapter.ListJob(ctx, limit, offset, filter)
}
