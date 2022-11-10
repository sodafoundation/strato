package util

import (
	"context"
	"encoding/hex"

	"github.com/micro/go-micro/v2/metadata"
	log "github.com/sirupsen/logrus"
	"github.com/xxtea/xxtea-go/xxtea"

	"github.com/soda/multi-cloud/api/pkg/common"
	. "github.com/soda/multi-cloud/s3/error"
)

var XXTEA_KEY = []byte("hehehehe")

func Decrypt(value string) (string, error) {
	bytes, err := hex.DecodeString(value)
	if err != nil {
		return "", err
	}
	return string(xxtea.Decrypt(bytes, XXTEA_KEY)), nil
}

func Encrypt(value string) string {
	return hex.EncodeToString(xxtea.Encrypt([]byte(value), XXTEA_KEY))
}

func GetCredentialFromCtx(ctx context.Context) (isAdmin bool, tenantId string, userId string, err error) {
	var ok bool
	var md map[string]string
	md, ok = metadata.FromContext(ctx)
	if !ok {
		log.Error("get metadata from ctx failed.")
		err = ErrInternalError
		return
	}

	isAdmin = false
	isAdminStr, _ := md[common.CTX_KEY_IS_ADMIN]
	if isAdminStr == common.CTX_VAL_TRUE {
		isAdmin = true
	}

	tenantId, ok = md[common.CTX_KEY_TENANT_ID]
	userId, ok = md[common.CTX_KEY_USER_ID]

	log.Debugf("isAdmin=%v, tenantId=%s, userId=%s, err=%v\n", isAdmin, tenantId, userId, err)
	return
}
