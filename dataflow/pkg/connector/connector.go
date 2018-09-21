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

package connector

import (
	"fmt"
	"regexp"

	"github.com/opensds/multi-cloud/dataflow/pkg/db"
	. "github.com/opensds/multi-cloud/dataflow/pkg/type"
)

func Create(conn *Connector) ErrCode {
	m, err := regexp.MatchString("[[:alnum:]-_.]+", conn.Name)
	if !m {
		fmt.Printf("Invalid connector name[%s], err:%v\n", conn.Name, err)
		return ERR_INVALID_CONN_NAME
	}
	//TO-DO check validation of connector

	return db.DbAdapter.CreateConnector(conn)
}

func Delete(id string, tenant string) ErrCode {
	return db.DbAdapter.DeleteConnector(id, tenant)
}

func Update(conn *Connector) ErrCode {
	m, err := regexp.MatchString("[[:alnum:]-_.]+", conn.Name)
	if !m {
		fmt.Printf("Invalid connector name[%s], err:%v\n", conn.Name, err)
		return ERR_INVALID_CONN_NAME
	}
	//TO-DO check validation of plan

	//update database
	return db.DbAdapter.UpdateConnector(conn)
}

func Get(name string, tenant string) ([]Connector, ErrCode) {
	m, err := regexp.MatchString("[[:alnum:]-_.]*", name)
	if !m {
		fmt.Printf("Invalid connector name[%s], err:%v\n", name, err)
		return nil, ERR_INVALID_CONN_NAME
	}

	return db.DbAdapter.GetConnector(name, tenant)
}
