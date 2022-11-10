// Copyright 2019 The soda Authors.
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
package tidbclient

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
	"github.com/xxtea/xxtea-go/xxtea"

	"github.com/soda/multi-cloud/api/pkg/common"
	. "github.com/soda/multi-cloud/s3/error"
	. "github.com/soda/multi-cloud/s3/pkg/meta/types"
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

func handleDBError(in error) (out error) {
	if in == nil {
		return nil
	}

	log.Errorf("db error:%v\n", in)
	if in == sql.ErrNoRows {
		out = ErrNoSuchKey
	} else {
		out = ErrDBError
	}
	return
}

func buildSql(ctx context.Context, filter map[string]string, sqltxt string) (string, []interface{}, error) {
	const MaxObjectList = 10000
	args := make([]interface{}, 0)

	prefix := filter[common.KPrefix]
	if prefix != "" {
		sqltxt += " and name like ?"
		args = append(args, prefix+"%")
		log.Debug("query prefix:", prefix)
	}
	if filter[common.KMarker] != "" {
		sqltxt += " and name >= ?"
		args = append(args, filter[common.KMarker])
		log.Debug("query marker:", filter[common.KMarker])
	}

	// lifecycle management may need to filter by LastModified
	if filter[common.KLastModified] != "" {
		var tmFilter map[string]string
		err := json.Unmarshal([]byte(filter[common.KLastModified]), &tmFilter)
		if err != nil {
			log.Errorf("unmarshal lastmodified value failed:%s\n", err)
			return sqltxt, args, ErrInternalError
		}
		log.Debugf("tmpFilter:%+v\n", tmFilter)

		for k, v := range tmFilter {
			var op string
			switch k {
			case "lt":
				op = "<"
			case "gt":
				op = ">"
			case "lte":
				op = "<="
			case "gte":
				op = ">="
			default:
				log.Infof("unsupport filter action:%s\n", k)
				return sqltxt, args, ErrInternalError
			}

			sqltxt += " and lastmodifiedtime " + op + " ?"
			args = append(args, v)
		}
	}

	// lifecycle management may need to filter by StorageTier
	if filter[common.KStorageTier] != "" {
		tier, err := strconv.Atoi(filter[common.KStorageTier])
		if err != nil {
			log.Errorf("invalid storage tier:%s\n", filter[common.KStorageTier])
			return sqltxt, args, ErrInternalError
		}

		sqltxt += " and tier <= ?"
		args = append(args, tier)
	}

	delimiter := filter[common.KDelimiter]
	if delimiter == "" {
		sqltxt += " order by bucketname,name,version limit ?"
		args = append(args, MaxObjectList)
	} else {
		num := len(strings.Split(prefix, delimiter))
		if prefix == "" {
			num += 1
		}

		sqltxt += " group by SUBSTRING_INDEX(name, ?, ?) order by bucketname, name,version limit ?"
		args = append(args, delimiter, num, MaxObjectList)
	}

	return sqltxt, args, nil
}

func VersionStr2UInt64(vers string) uint64 {
	vidByte, _ := hex.DecodeString(vers)
	decrByte := xxtea.Decrypt(vidByte, XXTEA_KEY)
	reVersion, _ := strconv.ParseUint(string(decrByte), 10, 64)
	version := math.MaxUint64 - reVersion

	return version
}
