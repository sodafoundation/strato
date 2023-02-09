package utils

import (
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/opensds/multi-cloud/metadata/pkg/model"
)

func AclMapper(grant *s3.Grant) *model.Access {
	access := &model.Access{}
	if grant.Grantee.DisplayName != nil {
		access.DisplayName = *grant.Grantee.DisplayName
	}
	if grant.Grantee.EmailAddress != nil {
		access.EmailAddress = *grant.Grantee.EmailAddress
	}
	if grant.Grantee.ID != nil {
		access.ID = *grant.Grantee.ID
	}
	if grant.Grantee.Type != nil {
		access.Type = *grant.Grantee.Type
	}
	if grant.Grantee.URI != nil {
		access.URI = *grant.Grantee.URI
	}
	if grant.Permission != nil {
		access.Permission = *grant.Permission
	}

	return access
}
