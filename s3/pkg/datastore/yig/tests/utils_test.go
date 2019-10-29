package tests

import (
	"sync"

	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/utils"
	. "gopkg.in/check.v1"
)

func (ys *YigSuite) TestGlobalId(c *C) {
	gi, err := utils.NewGlobalIdGen()
	c.Assert(err, Equals, nil)
	c.Assert(gi, Not(Equals), nil)
	ids := make(map[int64]int64)
	count := 8192

	for i := 0; i < count; i++ {
		id := gi.GetId()
		ids[id] = id
	}

	c.Assert(len(ids), Equals, count)
}

func (ys *YigSuite) TestGlobalIdConcurrent(c *C) {
	var wg sync.WaitGroup
	gi, _ := utils.NewGlobalIdGen()
	ids := make(map[int64]int64)
	count := 8192
	numThreads := 100
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
