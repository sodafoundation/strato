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
	"encoding/json"
	"github.com/micro/go-log"
	"github.com/opensds/multi-cloud/api/pkg/filters/context"
	"github.com/opensds/multi-cloud/dataflow/pkg/db"
	"github.com/opensds/multi-cloud/dataflow/pkg/kafka"
	. "github.com/opensds/multi-cloud/dataflow/pkg/model"
	datamover "github.com/opensds/multi-cloud/datamover/proto"
)

var abortMigration = "abort"

func Create(ctx *context.Context, job *Job) (*Job, error) {
	return db.DbAdapter.CreateJob(ctx, job)
}

func Get(ctx *context.Context, id string) (*Job, error) {
	log.Logf("get job %s", id)
	return db.DbAdapter.GetJob(ctx, id)
}

func List(ctx *context.Context, limit int, offset int, filter interface{}) ([]Job, error) {
	return db.DbAdapter.ListJob(ctx, limit, offset, filter)
}

func AbortJob(ctx *context.Context, id string) error {

	req := datamover.AbortJobRequest{Id: id}
	//data, err := json.Marshal(req)
	//if err != nil {
	//	log.Logf("Marshal run job request failed, err:%v\n", data)
	//	return err
	//}
	go sendabortJob(&req)
	return nil
	//return kafka.ProduceMsg(abortMigration, data)
}
func sendabortJob(req *datamover.AbortJobRequest) error {
	data, err := json.Marshal(*req)
	if err != nil {
		log.Logf("Marshal run job request failed, err:%v\n", data)
		return err
	}

	return kafka.ProduceMsg(abortMigration, data)
}
