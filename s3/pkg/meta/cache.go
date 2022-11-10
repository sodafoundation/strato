package meta

import (
	"errors"

	log "github.com/sirupsen/logrus"

	"github.com/soda/multi-cloud/s3/pkg/helper"
	"github.com/soda/multi-cloud/s3/pkg/meta/redis"
)

type CacheType int

const (
	NoCache CacheType = iota
	EnableCache
	SimpleCache
)

const (
	MSG_NOT_IMPL = "not implemented."
)

var cacheNames = [...]string{"NOCACHE", "EnableCache", "SimpleCache"}

type MetaCache interface {
	Close()
	Get(table redis.RedisDatabase, prefix, key string,
		onCacheMiss func() (helper.Serializable, error),
		onDeserialize func(map[string]string) (interface{}, error),
		willNeed bool) (value interface{}, err error)
	Remove(table redis.RedisDatabase, prefix, key string)
	GetCacheHitRatio() float64
	Keys(table redis.RedisDatabase, pattern string) ([]string, error)
	HGetInt64(table redis.RedisDatabase, prefix, key, field string) (int64, error)
	HMSet(table redis.RedisDatabase, prefix, key string, fields map[string]interface{}) (string, error)
	HIncrBy(table redis.RedisDatabase, prefix, key, field string, value int64) (int64, error)
}

type disabledMetaCache struct{}

type entry struct {
	table redis.RedisDatabase
	key   string
	value interface{}
}

func newMetaCache(myType CacheType) (m MetaCache) {

	log.Infof("Setting Up Metadata Cache: %s\n", cacheNames[int(myType)])
	if myType == SimpleCache {
		m := new(enabledSimpleMetaCache)
		m.Hit = 0
		m.Miss = 0
		return m
	}
	return &disabledMetaCache{}
}

func (m *disabledMetaCache) Get(table redis.RedisDatabase, prefix, key string,
	onCacheMiss func() (helper.Serializable, error),
	onDeserialize func(map[string]string) (interface{}, error),
	willNeed bool) (value interface{}, err error) {
	return onCacheMiss()
}

func (m *disabledMetaCache) Remove(table redis.RedisDatabase, prefix, key string) {
	return
}

func (m *disabledMetaCache) GetCacheHitRatio() float64 {
	return -1
}

func (m *disabledMetaCache) Keys(table redis.RedisDatabase, pattern string) ([]string, error) {
	return nil, errors.New(MSG_NOT_IMPL)
}

func (m *disabledMetaCache) HGetInt64(table redis.RedisDatabase, prefix, key, field string) (int64, error) {
	return 0, errors.New(MSG_NOT_IMPL)
}

func (m *disabledMetaCache) HMSet(table redis.RedisDatabase, prefix, key string, fields map[string]interface{}) (string, error) {
	return "", errors.New(MSG_NOT_IMPL)
}

func (m *disabledMetaCache) HIncrBy(table redis.RedisDatabase, prefix, key, field string, value int64) (int64, error) {
	return 0, errors.New(MSG_NOT_IMPL)
}

func (m *disabledMetaCache) Close() {
}

type enabledSimpleMetaCache struct {
	Hit  int64
	Miss int64
}

func (m *enabledSimpleMetaCache) Get(
	table redis.RedisDatabase,
	prefix, key string,
	onCacheMiss func() (helper.Serializable, error),
	onDeserialize func(map[string]string) (interface{}, error),
	willNeed bool) (value interface{}, err error) {

	log.Info("enabledSimpleMetaCache Get. table:", table, "key:", key)

	fields, err := redis.HGetAll(table, prefix, key)
	if err != nil {
		log.Error("enabledSimpleMetaCache Get err:", err, "table:", table, "key:", key)
	}
	if err == nil && fields != nil && len(fields) > 0 {
		value, err = onDeserialize(fields)
		m.Hit = m.Hit + 1
		return value, err
	}

	//if redis doesn't have the entry
	if onCacheMiss != nil {
		obj, err := onCacheMiss()
		if err != nil {
			return nil, err
		}

		if willNeed == true {
			values, err := obj.Serialize()
			if err != nil {
				log.Error("failed to serialize from %v", obj, " with err: ", err)
				return nil, err
			}
			_, err = redis.HMSet(table, prefix, key, values)
			if err != nil {
				log.Error("failed to set key: ", key, " with err: ", err)
				//do nothing, even if redis is down.
			}
		}
		m.Miss = m.Miss + 1
		return obj, nil
	}
	return nil, nil
}

func (m *enabledSimpleMetaCache) Remove(table redis.RedisDatabase, prefix, key string) {
	redis.Remove(table, prefix, key)
}

func (m *enabledSimpleMetaCache) GetCacheHitRatio() float64 {
	return float64(m.Hit) / float64(m.Hit+m.Miss)
}

func (m *enabledSimpleMetaCache) Keys(table redis.RedisDatabase, pattern string) ([]string, error) {
	return redis.Keys(table, pattern)
}

func (m *enabledSimpleMetaCache) HGetInt64(table redis.RedisDatabase, prefix, key, field string) (int64, error) {
	return redis.HGetInt64(table, prefix, key, field)
}

func (m *enabledSimpleMetaCache) HMSet(table redis.RedisDatabase, prefix, key string, fields map[string]interface{}) (string, error) {
	return redis.HMSet(table, prefix, key, fields)
}

func (m *enabledSimpleMetaCache) HIncrBy(table redis.RedisDatabase, prefix, key, field string, value int64) (int64, error) {
	return redis.HIncrBy(table, prefix, key, field, value)
}

func (m *enabledSimpleMetaCache) Close() {
	redis.Close()
}
