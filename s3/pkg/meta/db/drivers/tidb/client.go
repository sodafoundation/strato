package tidbclient

import (
	"context"
	"database/sql"
	"errors"
	"os"

	"github.com/globalsign/mgo/bson"
	_ "github.com/go-sql-driver/mysql"
	"github.com/micro/go-micro/metadata"
	"github.com/opensds/multi-cloud/api/pkg/common"
	. "github.com/opensds/multi-cloud/s3/error"
	log "github.com/sirupsen/logrus"
)

const MAX_OPEN_CONNS = 1024

type TidbClient struct {
	Client *sql.DB
}

func NewTidbClient(dbInfo string) *TidbClient {
	cli := &TidbClient{}
	conn, err := sql.Open("mysql", dbInfo)
	if err != nil {
		log.Errorf("connect to tidb failed, err:%v\n", err)
		os.Exit(1)
	}
	log.Info("connected to tidb ...")
	conn.SetMaxIdleConns(0)
	conn.SetMaxOpenConns(MAX_OPEN_CONNS)
	cli.Client = conn
	return cli
}

func UpdateContextFilter(ctx context.Context, m bson.M) error {
	// if context is admin, no need filter by tenantId.
	md, ok := metadata.FromContext(ctx)
	if !ok {
		log.Error("get context failed")
		return errors.New("get context failed")
	}

	isAdmin, _ := md[common.CTX_KEY_IS_ADMIN]
	if isAdmin != common.CTX_VAL_TRUE {
		tenantId, ok := md[common.CTX_KEY_TENANT_ID]
		if !ok {
			log.Error("get tenantid failed")
			return errors.New("get tenantid failed")
		}
		m[common.CTX_KEY_TENANT_ID] = tenantId
	}

	return nil
}

func handleDBError(in error) (out error) {
	if in == nil {
		return nil
	}

	if in == sql.ErrNoRows {
		out = ErrNoSuchKey
	} else {
		out = ErrDBError
	}
	return
}
