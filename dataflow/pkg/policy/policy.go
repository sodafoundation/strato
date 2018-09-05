package policy

import (
	"regexp"
	"github.com/micro/go-log"
	"github.com/opensds/go-panda/dataflow/pkg/db"
	. "github.com/opensds/go-panda/dataflow/pkg/type"
)

func Create(pol *Policy) ErrCode{
	m, err := regexp.MatchString("[[:alnum:]-_.]+", pol.Name)
	if !m {
		log.Logf("Invalid policy name[%s], err:%v\n", pol.Name,err)
		return ERR_INVALID_POLICY_NAME
	}
	//TO-DO check validation of policy
	return db.DbAdapter.CreatePolicy(pol)
}

func Delete(id string, tenantname string) ErrCode{
	/*m, err := regexp.MatchString("[[:alnum:]-_.]+", name)
	if !m {
		//log.Logf("Invalid policy name[%s], err:%v\n", name,err)
		log.Logf("Delete policy end, err = %d.", err)
		return ERR_INVALID_POLICY_NAME
	}*/

	return db.DbAdapter.DeletePolicy(id, tenantname)
}

//When update policy, policy id must be provided
func Update(pol *Policy) ErrCode{
	m, err := regexp.MatchString("[[:alnum:]-_.]+", pol.Name)
	if !m {
		log.Logf("Invalid policy name[%s], err:%v\n", pol.Name, err)
		return ERR_INVALID_POLICY_NAME
	}
	//TO-DO check validation of policy

	//update database
	return db.DbAdapter.UpdatePolicy(pol)
}

func Get(name string, tenant string) ([]Policy, ErrCode){
	m, err := regexp.MatchString("[[:alnum:]-_.]*", name)
	if !m {
		log.Logf("Invalid policy name[%s],err:%v\n", name, err)
		return nil,ERR_INVALID_POLICY_NAME
	}

	pols,errcode := db.DbAdapter.GetPolicy(name, tenant)
	if len(pols) == 0{
		log.Logf("Get nothing, policy name is %s, tenant is %s\n.", name, tenant)
	}

	return pols,errcode
}



