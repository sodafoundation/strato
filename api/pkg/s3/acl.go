package s3

import (
	"net/http"

	"github.com/emicklei/go-restful"

	"github.com/opensds/multi-cloud/api/pkg/common"
	. "github.com/opensds/multi-cloud/api/pkg/s3/datatype"
)

func getAclFromHeader(request *restful.Request) (acl Acl, err error) {
	acl.CannedAcl = request.HeaderParameter(common.REQUEST_HEADER_ACL)
	if acl.CannedAcl == "" {
		acl.CannedAcl = "private"
	}
	err = IsValidCannedAcl(acl)
	return
}

func getAclFromFormValues(formValues map[string]string) (acl Acl, err error) {
	headerfiedFormValues := make(http.Header)
	for key := range formValues {
		headerfiedFormValues.Add(key, formValues[key])
	}
	acl.CannedAcl = headerfiedFormValues.Get("acl")
	if acl.CannedAcl == "" {
		acl.CannedAcl = "private"
	}
	err = IsValidCannedAcl(acl)
	return
}
