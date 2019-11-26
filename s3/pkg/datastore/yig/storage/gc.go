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
package storage

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"time"

	. "github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/meta/types"
	log "github.com/sirupsen/logrus"
)

const (
	GC_OBJECT_LIMIT_NUM = 10000
)

type GcMgr struct {
	// context to cancel the operations.
	ctx        context.Context
	cancelFunc context.CancelFunc
	yig        *YigStorage
	loopTime   int64
	wg         sync.WaitGroup
}

func (gm *GcMgr) Start() {
	// query the current available cpus
	threadNum := runtime.GOMAXPROCS(0)
	gm.wg.Add(1)
	go func() {
		for {
			select {
			case <-gm.ctx.Done():
				log.Infof("GcMgr is stopping.")
				gm.wg.Done()
				return
			case <-time.After(time.Second * time.Duration(gm.loopTime)):
			}
			var chs []<-chan *GcObjectResult
			// get all the gc objects for this loop
			gcChan := gm.QueryGcObjectStream()
			// by default, we will start the go routines with the number of available cpus.
			for i := 0; i < threadNum; i++ {
				// remove the gc objects from ceph storage
				ch := gm.CreateObjectDeleteStream(gcChan)
				chs = append(chs, ch)
			}
			// clear the removed gc objects from gc table.
			chResult := gm.CreateGcObjectRecordCleanStream(chs...)
			// record the success or failure.
			for result := range chResult {
				if result.ErrNo == ErrNoErr {
					log.Debugf("succeed to remove object: %s", result.ObjectId)
					continue
				}
				log.Errorf("failed to remove object: %s, err: %s", result.ObjectId, result.Err)
			}
		}
	}()
}

func (gm *GcMgr) Stop() {
	log.Infof("try to stop GcMgr...")
	gm.cancelFunc()
	gm.wg.Wait()
	log.Infof("GcMgr has stopped.")
}

func (gm *GcMgr) QueryGcObjectStream() <-chan *types.GcObject {
	out := make(chan *types.GcObject)
	go func() {
		defer close(out)
		start := int64(0)
		for {
			gcObjects, err := gm.yig.MetaStorage.GetGcObjects(start, GC_OBJECT_LIMIT_NUM)
			if err != nil {
				log.Errorf("failed to get gc objects(%d), err: %v", start, err)
				return
			}
			if gcObjects == nil || len(gcObjects) == 0 {
				log.Debugf("got empty gc objects(%d)", start)
				return
			}
			// set the next marker to query the gc objects.
			start = gcObjects[len(gcObjects)-1].Id + 1
			for _, o := range gcObjects {
				select {
				case out <- o:
				case <-gm.ctx.Done():
					return
				}
			}
			// check whether it is finished to read in this loop.
			if len(gcObjects) < GC_OBJECT_LIMIT_NUM {
				return
			}
		}
	}()

	return out
}

type GcObjectResult struct {
	ErrNo    S3ErrorCode
	Err      error
	Id       int64
	ObjectId string
}

func (gm *GcMgr) CreateObjectDeleteStream(in <-chan *types.GcObject) <-chan *GcObjectResult {
	out := make(chan *GcObjectResult)

	go func() {
		defer close(out)
		for o := range in {
			result := &GcObjectResult{
				Id:       o.Id,
				ObjectId: o.ObjectId,
			}
			ceph, ok := gm.yig.DataStorage[o.Location]
			if !ok {
				log.Errorf("cannot find the ceph storage for gc object(%s, %s, %s)", o.Location, o.Pool, o.ObjectId)
				result.ErrNo = ErrNoSuchKey
				result.Err = errors.New("cannot find the ceph storage")
				select {
				case out <- result:
				case <-gm.ctx.Done():
					return
				}
				continue
			}
			err := ceph.Remove(o.Pool, o.ObjectId)
			if err != nil {
				log.Errorf("failed to remove object(%s, %s, %s) from ceph, err: %v", o.Location, o.Pool, o.ObjectId, err)
				result.ErrNo = ErrInternalError
				result.Err = err
				select {
				case out <- result:
				case <-gm.ctx.Done():
					return
				}
				return
			}
			result.Err = nil
			result.ErrNo = ErrNoErr
			select {
			case out <- result:
			case <-gm.ctx.Done():
				return
			}
		}
	}()

	return out
}

func (gm *GcMgr) CreateGcObjectRecordCleanStream(in ...<-chan *GcObjectResult) <-chan *GcObjectResult {
	wg := sync.WaitGroup{}
	out := make(chan *GcObjectResult)
	clearfunc := func(ch <-chan *GcObjectResult) {
		defer wg.Done()
		count := 0
		var gcObjects []*types.GcObject
		for result := range ch {
			// check error of the result
			if result.ErrNo != ErrNoErr {
				select {
				case out <- result:
					continue
				case <-gm.ctx.Done():
					return
				}
			}
			//batch clean the gc objects from gc table.
			gcObj := &types.GcObject{
				ObjectId: result.ObjectId,
			}
			gcObjects = append(gcObjects, gcObj)
			count += 1
			if count >= GC_OBJECT_LIMIT_NUM {
				err := gm.yig.MetaStorage.DeleteGcObjects(gcObjects...)
				if err != nil {
					for _, o := range gcObjects {
						clearResult := &GcObjectResult{
							ErrNo:    ErrInternalError,
							Err:      err,
							ObjectId: o.ObjectId,
						}
						select {
						case out <- clearResult:
						case <-gm.ctx.Done():
							return
						}
					}
				} else {
					for _, o := range gcObjects {
						clearResult := &GcObjectResult{
							ErrNo:    ErrNoErr,
							Err:      nil,
							ObjectId: o.ObjectId,
						}
						select {
						case out <- clearResult:
						case <-gm.ctx.Done():
							return
						}
					}
				}
				// free the buffer slice and re-calculate again.
				count = 0
				gcObjects = nil
			}
		}
		// clear the remaining gc objects.
		if len(gcObjects) > 0 {
			err := gm.yig.MetaStorage.DeleteGcObjects(gcObjects...)
			if err != nil {
				for _, o := range gcObjects {
					clearResult := &GcObjectResult{
						ErrNo:    ErrInternalError,
						Err:      err,
						ObjectId: o.ObjectId,
					}
					select {
					case out <- clearResult:
					case <-gm.ctx.Done():
						return
					}
				}
			} else {
				for _, o := range gcObjects {
					clearResult := &GcObjectResult{
						ErrNo:    ErrNoErr,
						Err:      nil,
						ObjectId: o.ObjectId,
					}
					select {
					case out <- clearResult:
					case <-gm.ctx.Done():
						return
					}
				}
			}
		}
	}
	for _, ch := range in {
		wg.Add(1)
		go clearfunc(ch)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func NewGcMgr(ctx context.Context, yig *YigStorage, loopTime int64) *GcMgr {
	cancelCtx, cancelFunc := context.WithCancel(ctx)
	return &GcMgr{
		ctx:        cancelCtx,
		cancelFunc: cancelFunc,
		yig:        yig,
		loopTime:   loopTime,
		wg:         sync.WaitGroup{},
	}
}