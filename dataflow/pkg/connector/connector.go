package connector

import (
	"regexp"
	"fmt"
	"github.com/opensds/go-panda/dataflow/pkg/db"
	. "github.com/opensds/go-panda/dataflow/pkg/type"
)
func Create(conn *Connector) ErrCode {
	m, err := regexp.MatchString("[[:alnum:]-_.]+", conn.Name)
	if !m {
		fmt.Printf("Invalid connector name[%s], err:%v\n", conn.Name,err)
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
		fmt.Printf("Invalid connector name[%s], err:%v\n", conn.Name,err)
		return ERR_INVALID_CONN_NAME
	}
	//TO-DO check validation of plan

	//update database
	return db.DbAdapter.UpdateConnector(conn)
}

func Get(name string, tenant string) ([]Connector, ErrCode) {
	m, err := regexp.MatchString("[[:alnum:]-_.]*", name)
	if !m {
		fmt.Printf("Invalid connector name[%s], err:%v\n", name,err)
		return nil,ERR_INVALID_CONN_NAME
	}

	return db.DbAdapter.GetConnector(name, tenant)
}
