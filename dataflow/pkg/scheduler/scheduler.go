// Copyright 2019 The soda Authors.
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
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/micro/go-micro/v2/metadata"
	"github.com/robfig/cron"
	log "github.com/sirupsen/logrus"

	"github.com/soda/multi-cloud/api/pkg/common"
	"github.com/soda/multi-cloud/dataflow/pkg/db"
	"github.com/soda/multi-cloud/dataflow/pkg/model"
	"github.com/soda/multi-cloud/dataflow/pkg/plan"
	"github.com/soda/multi-cloud/dataflow/pkg/scheduler/lifecycle"
	"github.com/soda/multi-cloud/dataflow/pkg/scheduler/trigger"
)

func LoadAllPlans() {
	ctx := metadata.NewContext(context.Background(), map[string]string{
		common.CTX_KEY_IS_ADMIN: strconv.FormatBool(true),
	})

	offset := model.DefaultOffset
	limit := model.DefaultLimit

	planNum := 0
	for offset == 0 || planNum > 0 {
		plans, err := db.DbAdapter.ListPlan(ctx, limit, offset, nil)
		if err != nil {
			log.Errorf("Get all plan faild, %v", err)
			break
		}
		log.Infof("scheduler: planNum=%d\n", planNum)
		planNum = len(plans)
		if planNum == 0 {
			break
		} else {
			offset += planNum
		}
		for _, p := range plans {
			if p.PolicyId == "" || !p.PolicyEnabled {
				continue
			}
			e := plan.NewPlanExecutor(&p)
			err := trigger.GetTriggerMgr().Add(ctx, &p, e)
			if err != nil {
				log.Errorf("Load plan(%s) to trigger failed, %v", p.Id.Hex(), err)
				continue
			}
			log.Infof("Load plan(%s) to trigger success", p.Id.Hex())
		}
	}
}

//This scheduler will scan all buckets periodically to get lifecycle rules, and scheduling according to these rules.
func LoadLifecycleScheduler() error {
	spec := os.Getenv("LIFECYCLE_CRON_CONFIG")
	log.Infof("Value of LIFECYCLE_CRON_CONFIG is: %s\n", spec)

	//TODO: Check the validation of spec
	cn := cron.New()
	//0 */10 * * * ?
	if err := cn.AddFunc(spec, lifecycle.ScheduleLifecycle); err != nil {
		log.Errorf("add lifecyecle scheduler to cron trigger failed: %v.\n", err)
		return fmt.Errorf("add lifecyecle scheduler to cron trigger failed: %v", err)
	}
	cn.Start()

	log.Info("add lifecycle scheduler to cron trigger successfully.")
	return nil
}
