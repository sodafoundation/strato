package crypto

import (
	"github.com/opensds/multi-cloud/s3/pkg/helper"
	log "github.com/sirupsen/logrus"
)

func NewKMS() KMS {
	switch helper.CONFIG.KMS.Type {
	case "vault":
		c, err := NewVaultConfig()
		if err != nil {
			panic("read kms vault err:" + err.Error())
		}
		vault, err := NewVault(c)
		if err != nil {
			panic("create vault err:" + err.Error())
		}
		return vault

	//extention case here

	default:
		log.Error("not support kms type", helper.CONFIG.KMS.Type)
		return nil
	}
}
