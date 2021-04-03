// Copyright 2019 The OpenSDS Authors.
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
package meta

import (
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"

	. "github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/pkg/helper"
	"github.com/opensds/multi-cloud/s3/pkg/meta/redis"
	. "github.com/opensds/multi-cloud/s3/pkg/meta/types"
	pb "github.com/opensds/multi-cloud/s3/proto"
)

const (
	BUCKET_CACHE_PREFIX = "bucket:"
	USER_CACHE_PREFIX   = "user:"
)

// Note the usage info got from this method is possibly not accurate because we don't
// invalid cache when updating usage. For accurate usage info, use `GetUsage()`
func (m *Meta) GetBucket(ctx context.Context, bucketName string, willNeed bool) (bucket *Bucket, err error) {
	getBucket := func() (b helper.Serializable, err error) {
		bt, err := m.Db.GetBucket(ctx, bucketName)
		log.Info("GetBucket CacheMiss. bucket:", bucketName)
		return bt, err
	}

	toBucket := func(fields map[string]string) (interface{}, error) {
		b := &Bucket{Bucket: &pb.Bucket{}}
		return b.Deserialize(fields)
	}

	b, err := m.Cache.Get(redis.BucketTable, BUCKET_CACHE_PREFIX, bucketName, getBucket, toBucket, willNeed)
	if err != nil {
		if err == ErrNoSuchKey {
			err = ErrNoSuchBucket
		} else if err != ErrDBError {
			err = ErrInternalError
		}
		log.Errorf("get bucket failed:%v\n", err)
		return
	}
	bucket, ok := b.(*Bucket)
	if !ok {
		log.Error("Cast b failed:", b)
		err = ErrInternalError
		return
	}
	return bucket, nil
}

/*
* init bucket usage cache when meta is newed.
*
 */
func (m *Meta) InitBucketUsageCache() error {
	// the map contains the bucket usage which are not in cache.
	bucketUsageMap := make(map[string]*Bucket)
	// the map contains the bucket usage which are in cache and will be synced into database.
	bucketUsageCacheMap := make(map[string]int64)
	// the usage in buckets table is accurate now.

	buckets, err := m.Db.GetBuckets(context.Background())
	if err != nil {
		log.Error("failed to get buckets from db. err: ", err)
		return err
	}

	// init the bucket usage key in cache.
	for _, bucket := range buckets {
		bucketUsageMap[bucket.Name] = bucket
	}

	// try to get all bucket usage keys from cache.
	pattern := fmt.Sprintf("%s*", BUCKET_CACHE_PREFIX)
	bucketsInCache, err := m.Cache.Keys(redis.BucketTable, pattern)
	if err != nil {
		log.Error("failed to get bucket usage from cache, err: ", err)
		return err
	}

	if len(bucketsInCache) > 0 {
		// query all usages from cache.
		for _, bic := range bucketsInCache {
			usage, err := m.Cache.HGetInt64(redis.BucketTable, BUCKET_CACHE_PREFIX, bic, FIELD_NAME_USAGE)
			if err != nil {
				log.Error("failed to get usage for bucket: ", bic, " with err: ", err)
				continue
			}
			// add the to be synced usage.
			bucketUsageCacheMap[bic] = usage
			if _, ok := bucketUsageMap[bic]; ok {
				// if the key already exists in cache, then delete it from map
				delete(bucketUsageMap, bic)
			}
		}

	}

	// init the bucket usage in cache.
	if len(bucketUsageMap) > 0 {
		for _, bk := range bucketUsageMap {
			fields, err := bk.Serialize()
			if err != nil {
				log.Error("failed to serialize for bucket: ", bk.Name, " with err: ", err)
				return err
			}
			_, err = m.Cache.HMSet(redis.BucketTable, BUCKET_CACHE_PREFIX, bk.Name, fields)
			if err != nil {
				log.Error("failed to set bucket to cache: ", bk.Name, " with err: ", err)
				return err
			}
		}

	}
	// sync the buckets usage in cache into database.
	if len(bucketUsageCacheMap) > 0 {
		err = m.Db.UpdateUsages(context.Background(), bucketUsageCacheMap, nil)
		if err != nil {
			log.Error("failed to sync usages to database, err: ", err)
			return err
		}
	}
	return nil
}

/*func (m *Meta) bucketUsageSync(event SyncEvent) error {
	bu := &BucketUsageEvent{}
	err := helper.MsgPackUnMarshal(event.Data.([]byte), bu)
	if err != nil {
		log.Error("failed to unpack from event data to BucketUsageEvent, err: %v", err)
		return err
	}

	err = m.Db.UpdateUsage(bu.BucketName, bu.Usage, nil)
	if err != nil {
		log.Error("failed to update bucket usage ", bu.Usage, " to bucket: ", bu.BucketName,
			" err: ", err)
		return err
	}

	log.Infof("succeed to update bucket usage ", bu.Usage, " for bucket: ", bu.BucketName)
	return nil
}*/

func AddBucketUsageSyncEvent(bucketName string, usage int64) {
	bu := &BucketUsageEvent{
		Usage:      usage,
		BucketName: bucketName,
	}
	data, err := helper.MsgPackMarshal(bu)
	if err != nil {
		log.Errorf("failed to package bucket usage event for bucket %s with usage %d, err: %v",
			bucketName, usage, err)
		return
	}
	if MetaSyncQueue != nil {
		event := SyncEvent{
			Type: SYNC_EVENT_TYPE_BUCKET_USAGE,
			Data: data,
		}
		MetaSyncQueue <- event
	}
}
