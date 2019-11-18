package config

import (
	"errors"
	"strings"

	"github.com/spf13/viper"
)

const (
	DEFAULT_DB_MAX_IDLE_CONNS = 1024
	DEFAULT_DB_MAX_OPEN_CONNS = 1024
)

type Config struct {
	Endpoint   EndpointConfig
	Log        LogConfig
	StorageCfg StorageConfig
	Database   DatabaseConfig
}

func (config *Config) Parse() error {
	endpoint := viper.GetStringMap("endpoint")
	log := viper.GetStringMap("log")
	storageCfg := viper.GetStringMap("storage")
	db := viper.GetStringMap("database")

	(&config.Endpoint).Parse(endpoint)
	(&config.Log).Parse(log)
	(&config.StorageCfg).Parse(storageCfg)
	(&config.Database).Parse(db)

	return nil
}

type CommonConfig struct {
	Log   LogConfig
	Cache CacheConfig
}

func (cc *CommonConfig) Parse() error {
	log := viper.GetStringMap("log")
	cache := viper.GetStringMap("cache")

	cc.Log.Parse(log)
	cc.Cache.Parse(cache)

	return nil
}

type EndpointConfig struct {
	Url       string
	MachineId int
}

func (ec *EndpointConfig) Parse(vals map[string]interface{}) error {
	if url, ok := vals["url"]; ok {
		ec.Url = url.(string)
		return nil
	} else {
		return errors.New("no url found")
	}

	if id, ok := vals["machine_id"]; ok {
		ec.MachineId = id.(int)
		return nil
	}
	return errors.New("no machine_id found")
}

type LogConfig struct {
	Path  string
	Level int
}

func (lc *LogConfig) Parse(vals map[string]interface{}) error {
	if p, ok := vals["log_path"]; ok {
		lc.Path = p.(string)
	}
	if l, ok := vals["log_level"]; ok {
		lc.Level = int(l.(int64))
	}
	return nil
}

type StorageConfig struct {
	CephPath string
}

func (sc *StorageConfig) Parse(vals map[string]interface{}) error {
	if p, ok := vals["ceph_dir"]; ok {
		sc.CephPath = p.(string)
	}
	return nil
}

type CacheConfig struct {
	Mode              int
	Nodes             []string
	Master            string
	Address           string
	Password          string
	ConnectionTimeout int
	ReadTimeout       int
	WriteTimeout      int
	KeepAlive         int
	PoolMaxIdle       int
	PoolIdleTimeout   int
}

func (cc *CacheConfig) Parse(vals map[string]interface{}) error {
	if m, ok := vals["redis_mode"]; ok {
		cc.Mode = int(m.(int64))
	}
	if n, ok := vals["redis_nodes"]; ok {
		nodes := n.(string)
		cc.Nodes = strings.Split(nodes, ",")
	}
	if master, ok := vals["redis_master_name"]; ok {
		cc.Master = master.(string)
	}
	if addr, ok := vals["redis_address"]; ok {
		cc.Address = addr.(string)
	}
	if password, ok := vals["redis_password"]; ok {
		cc.Password = password.(string)
	}
	if ct, ok := vals["redis_connect_timeout"]; ok {
		cc.ConnectionTimeout = int(ct.(int64))
	}
	if rt, ok := vals["redis_read_timeout"]; ok {
		cc.ReadTimeout = int(rt.(int64))
	}
	if wt, ok := vals["redis_write_timeout"]; ok {
		cc.WriteTimeout = int(wt.(int64))
	}
	if ka, ok := vals["redis_keepalive"]; ok {
		cc.KeepAlive = int(ka.(int64))
	}
	if pa, ok := vals["redis_pool_max_idle"]; ok {
		cc.PoolMaxIdle = int(pa.(int64))
	}
	if pt, ok := vals["redis_pool_idle_timeout"]; ok {
		cc.PoolIdleTimeout = int(pt.(int64))
	}

	return nil
}

type DatabaseConfig struct {
	DbType       string
	DbUrl        string
	DbPassword   string
	MaxIdleConns int
	MaxOpenConns int
}

func (dc *DatabaseConfig) Parse(vals map[string]interface{}) error {
	dc.MaxIdleConns = DEFAULT_DB_MAX_IDLE_CONNS
	dc.MaxOpenConns = DEFAULT_DB_MAX_OPEN_CONNS

	if dt, ok := vals["db_type"]; ok {
		dc.DbType = dt.(string)
	}
	if du, ok := vals["db_url"]; ok {
		dc.DbUrl = du.(string)
	}
	if dp, ok := vals["db_password"]; ok {
		dc.DbPassword = dp.(string)
	}
	if mi, ok := vals["db_maxidleconns"]; ok {
		dc.MaxIdleConns = mi.(int)
	}
	if mc, ok := vals["db_maxopenconns"]; ok {
		dc.MaxOpenConns = mc.(int)
	}

	return nil
}
