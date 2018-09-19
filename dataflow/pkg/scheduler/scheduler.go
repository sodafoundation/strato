// Copyright (c) 2018 Huawei Technologies Co., Ltd. All Rights Reserved.
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

package scheduler

import (
	"github.com/opensds/go-panda/datamover/proto"
	"github.com/opensds/go-panda/dataflow/pkg/plan"
	"github.com/micro/go-log"
	"github.com/opensds/go-panda/dataflow/pkg/db"
	"github.com/opensds/go-panda/dataflow/pkg/scheduler/trigger"
)


func LoadAllPlans(service datamover.DatamoverService) {
	plans, err := db.DbAdapter.GetPlan("all", "tenant")
	if err != nil {
		log.Logf("Get all plan faild, %v", err)
	}
	for _, p := range(plans) {
		e := plan.NewPlanExecutor(service, &p)
		err := trigger.GetTriggerMgr().Add(&p, e)
		if err != nil {
			log.Logf("Load plan(%s) to trigger filed, %v", p.Id.Hex(), err)
			continue
		}
		log.Logf("Load plan(%s) to trigger success", p.Id.Hex())
	}
}