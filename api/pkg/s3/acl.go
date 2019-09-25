package s3

import (
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
