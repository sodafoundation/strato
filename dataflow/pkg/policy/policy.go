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

package policy

import (
	"reflect"
	"regexp"

	"github.com/micro/go-log"
	"github.com/opensds/multi-cloud/dataflow/pkg/db"
	. "github.com/opensds/multi-cloud/dataflow/pkg/type"
)

func Create(pol *Policy) error {
	m, err := regexp.MatchString("[[:alnum:]-_.]+", pol.Name)
	if !m || pol.Name == "all" {
		log.Logf("Invalid policy name[%s], err:%v\n", pol.Name, err)
		return ERR_INVALID_POLICY_NAME
	}

	//TODO check validation of policy
	return db.DbAdapter.CreatePolicy(pol)
}

func Delete(id string, tenantname string) error {
	return db.DbAdapter.DeletePolicy(id, tenantname)
}

//When update policy, policy id must be provided
func Update(pol *Policy) error {
	if pol.Name != "" {
		m, err := regexp.MatchString("[[:alnum:]-_.]+", pol.Name)
		if !m || pol.Name == "all" {
			log.Logf("Invalid policy name[%s], err:%v\n", pol.Name, err)
			return ERR_INVALID_POLICY_NAME
		}
	}

	curPol, err := db.DbAdapter.GetPolicyById(pol.Id.Hex(), pol.Tenant)
	if err != nil {
		log.Logf("Update policy failed, err: connot get the policy(%v).\n", err.Error())
		return err
	}

	if pol.Name != "" {
		curPol.Name = pol.Name
	}
	if pol.Description != "" {
		curPol.Description = pol.Description
	}
	if !reflect.DeepEqual(pol.Schedule, Schedule{}) {
		curPol.Schedule = pol.Schedule
	}

	//TODO check validation of policy

	//update database
	return db.DbAdapter.UpdatePolicy(curPol)
}

func Get(name string, tenant string) ([]Policy, error) {
	m, err := regexp.MatchString("[[:alnum:]-_.]*", name)
	if !m {
		log.Logf("Invalid policy name[%s],err:%v\n", name, err)
		return nil, ERR_INVALID_POLICY_NAME
	}

	pols, errcode := db.DbAdapter.GetPolicy(name, tenant)
	if len(pols) == 0 {
		log.Logf("Get nothing, policy name is %s, tenant is %s\n.", name, tenant)
	}

	return pols, errcode
}
