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

package trigger

import (
	"sync"
	"fmt"
	"errors"

	"github.com/opensds/multi-cloud/dataflow/pkg/db"
	"github.com/micro/go-log"
	"github.com/opensds/multi-cloud/dataflow/pkg/type"
)

var mgr *Manager
var once sync.Once


func GetTriggerMgr() *Manager {
	once.Do(func() {
		mgr = &Manager{}
	})
	return mgr
}

type Manager struct {}

func (m *Manager) Add(plan *_type.Plan, executer Executer) error  {

	if plan.PolicyId == "" {
		return fmt.Errorf("specifed plan(%s) does not have policy", plan.Id.Hex())
	}
	policy, err := db.DbAdapter.GetPolicyById(plan.PolicyId, plan.Tenant)
	if err !=  nil {
		log.Logf("Get specified policy(%s) failed", plan.PolicyId)
		return err
	}

	tg := GetTrigger(policy.Schedule.Type)
	if tg == nil {
		msg := fmt.Sprintf("specifed trigger type(%s) is not exist", policy.Schedule.Type)
		log.Log(msg)
		return errors.New(msg)
	}

	return tg.Add(plan.Id.Hex(), policy.Schedule.TriggerProperties, executer)
}

func (m *Manager) Update(plan *_type.Plan, executer Executer) error {
	if plan.PolicyId == "" {
		return fmt.Errorf("specifed plan(%s) does not have policy", plan.Id.Hex())
	}
	policy, err := db.DbAdapter.GetPolicyById(plan.PolicyId, plan.Tenant)
	if err !=  nil {
		log.Logf("Get specified policy(%s) failed", plan.PolicyId)
		return err
	}

	tg := GetTrigger(policy.Schedule.Type)
	if tg == nil {
		msg := fmt.Sprintf("specifed trigger type(%s) is not exist", policy.Schedule.Type)
		log.Log(msg)
		return errors.New(msg)
	}
	return tg.Update(plan.Id.Hex(), policy.Schedule.TriggerProperties, executer)
}


func (m *Manager) Remove(plan *_type.Plan) error  {
	if plan.PolicyId == "" {
		return fmt.Errorf("specifed plan(%s) does not have policy", plan.Id.Hex())
	}
	policy, err := db.DbAdapter.GetPolicyById(plan.PolicyId, plan.Tenant)
	if err !=  nil {
		log.Logf("Get specified policy(%s) failed", plan.PolicyId)
		return err
	}
	tg := GetTrigger(policy.Schedule.Type)
	if tg == nil {
		msg := fmt.Sprintf("specifed trigger type(%s) is not exist", policy.Schedule.Type)
		log.Log(msg)
		return errors.New(msg)
	}

	return tg.Remove(plan.Id.Hex())
}
