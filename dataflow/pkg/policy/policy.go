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

package policy

import (
	"regexp"

	"encoding/json"

	log "github.com/sirupsen/logrus"
	"github.com/opensds/multi-cloud/api/pkg/filters/context"
	"github.com/opensds/multi-cloud/dataflow/pkg/db"
	. "github.com/opensds/multi-cloud/dataflow/pkg/model"
	"github.com/opensds/multi-cloud/dataflow/pkg/plan"
	"github.com/opensds/multi-cloud/dataflow/pkg/scheduler/trigger"
)

func Create(ctx *context.Context, pol *Policy) (*Policy, error) {
	m, err := regexp.MatchString("[[:alnum:]-_.]+", pol.Name)
	if !m || pol.Name == "all" {
		log.Errorf("Invalid policy name[%s], err:%v\n", pol.Name, err)
		return nil, ERR_INVALID_POLICY_NAME
	}

	return db.DbAdapter.CreatePolicy(ctx, pol)
}

func Delete(ctx *context.Context, id string) error {
	return db.DbAdapter.DeletePolicy(ctx, id)
}

//When update policy, policy id must be provided
func Update(ctx *context.Context, policyId string, updateMap map[string]interface{}) (*Policy, error) {

	curPol, err := db.DbAdapter.GetPolicy(ctx, policyId)
	if err != nil {
		log.Errorf("Update policy failed, err: connot get the policy(%v).\n", err.Error())
		return nil, err
	}

	if v, ok := updateMap["name"]; ok {
		name := v.(string)
		m, err := regexp.MatchString("[[:alnum:]-_.]+", name)
		if !m {
			log.Errorf("Invalid policy name[%s],err:", name, err) //cannot use all as name
			return nil, ERR_INVALID_PLAN_NAME
		}
		curPol.Name = name
	}

	if v, ok := updateMap["description"]; ok {
		curPol.Description = v.(string)
	}

	var updateInTrigger = false
	if v, ok := updateMap["schedule"]; ok {
		b, _ := json.Marshal(v)
		curPol.Schedule = Schedule{}
		json.Unmarshal(b, &curPol.Schedule)
		updateInTrigger = true
	}

	//update database
	resp, err := db.DbAdapter.UpdatePolicy(ctx, curPol)
	if err != nil {
		return nil, err
	}

	if updateInTrigger {
		if err := updatePolicyInTrigger(ctx, resp); err != nil {
			return nil, err
		}
	}

	return resp, nil
}

func updatePolicyInTrigger(ctx *context.Context, policy *Policy) error {
	offset := DefaultOffset
	limit := DefaultLimit

	t := trigger.GetTriggerMgr()
	planNum := 0
	for offset == 0 || planNum > 0 {
		plans, err := db.DbAdapter.GetPlanByPolicy(ctx, policy.Id.Hex(), limit, offset)
		if err != nil {
			log.Errorf("Get plan by policy id(%s) failed, err", policy.Id.Hex(), err)
			return err
		}
		planNum = len(plans)
		if planNum == 0 {
			break
		} else {
			offset += planNum
		}
		for _, p := range plans {
			if !p.PolicyEnabled {
				continue // Policy is not enabled, ignore it
			}
			exe := plan.NewPlanExecutor(ctx, &p)
			if err := t.Update(ctx, &p, exe); err != nil {
				return err
			}
		}
	}

	return nil
}

func Get(ctx *context.Context, id string) (*Policy, error) {
	m, err := regexp.MatchString("[[:alnum:]-_.]*", id)
	if !m {
		log.Errorf("Invalid policy id[%s],err:%v\n", id, err)
		return nil, ERR_INVALID_POLICY_NAME
	}

	return db.DbAdapter.GetPolicy(ctx, id)
}

func List(ctx *context.Context) ([]Policy, error) {
	return db.DbAdapter.ListPolicy(ctx)
}
