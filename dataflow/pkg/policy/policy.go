package policy

import (
	"regexp"
	"fmt"
	"github.com/opensds/go-panda/dataflow/pkg/db"
	. "github.com/opensds/go-panda/dataflow/pkg/type"
)

func Create(pol *Policy) ErrCode{
	m, err := regexp.MatchString("[[:alnum:]-_.]+", pol.Name)
	if !m {
		fmt.Printf("Invalid policy name[%s], err:%v\n", pol.Name,err)
		return ERR_INVALID_POLICY_NAME
	}
	//TO-DO check validation of policy
	return db.DbAdapter.CreatePolicy(pol)
}

func Delete(name string, tenantname string) ErrCode{
	m, err := regexp.MatchString("[[:alnum:]-_.]+", name)
	if !m {
		fmt.Printf("Invalid policy name[%s], err:%v\n", name,err)
		return ERR_INVALID_POLICY_NAME
	}

	return db.DbAdapter.DeletePolicy(name, tenantname)
}

//When update policy, policy id must be provided
func Update(pol *Policy) ErrCode{
	m, err := regexp.MatchString("[[:alnum:]-_.]+", pol.Name)
	if !m {
		fmt.Printf("Invalid policy name[%s], err:%v\n", pol.Name, err)
		return ERR_INVALID_POLICY_NAME
	}
	//TO-DO check validation of policy

	//update database
	return db.DbAdapter.UpdatePolicy(pol)
}

func Get(name string, tenant string) ([]Policy, ErrCode){
	m, err := regexp.MatchString("[[:alnum:]-_.]*", name)
	if !m {
		fmt.Printf("Invalid policy name[%s],err:%v\n", name, err)
		return nil,ERR_INVALID_POLICY_NAME
	}

	pols,errcode := db.DbAdapter.GetPolicy(name, tenant)
	if len(pols) == 0{
		fmt.Printf("Get nothing, policy name is %s, tenant is %s\n.", name, tenant)
	}

	return pols,errcode
}



