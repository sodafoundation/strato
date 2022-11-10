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

package tests

import (
	"sync"

	. "gopkg.in/check.v1"

	"github.com/soda/multi-cloud/s3/pkg/datastore/yig/utils"
)

const (
	NUM_ID_NEEDED            = 8192
	NUM_CONCURRENT_REQUESTOR = 100
)

func (ys *YigSuite) TestGlobalId(c *C) {
	gi, err := utils.NewGlobalIdGen(0)
	c.Assert(err, Equals, nil)
	c.Assert(gi, Not(Equals), nil)
	ids := make(map[int64]int64)

	for i := 0; i < NUM_ID_NEEDED; i++ {
		id := gi.GetId()
		ids[id] = id
	}

	c.Assert(len(ids), Equals, NUM_ID_NEEDED)
}

func (ys *YigSuite) TestGlobalIdConcurrent(c *C) {
	var wg sync.WaitGroup
	gi, _ := utils.NewGlobalIdGen(0)
	ids := make(map[int64]int64)
	count := NUM_ID_NEEDED
	numThreads := NUM_CONCURRENT_REQUESTOR
	idsChan := make(chan int64)
	wg.Add(numThreads)

	funcGen := func(loop int) {
		for i := 0; i < loop; i++ {
			id := gi.GetId()
			idsChan <- id
		}
		wg.Done()
	}

	for i := 0; i < numThreads; i++ {
		go funcGen(count)
	}

	go func() {
		wg.Wait()
		close(idsChan)
	}()

	for id := range idsChan {
		ids[id] = id
	}

	c.Assert(len(ids), Equals, numThreads*count)
}
