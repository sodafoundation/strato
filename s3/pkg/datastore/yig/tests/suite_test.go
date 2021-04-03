package tests

import (
	"testing"

	. "gopkg.in/check.v1"

	_ "github.com/opensds/multi-cloud/s3/pkg/datastore"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/driver"
)

func Test(t *testing.T) { TestingT(t) }

type YigSuite struct {
}

var _ = Suite(&YigSuite{})

func (ys *YigSuite) SetUpSuite(c *C) {
}

func (ys *YigSuite) TearDownSuite(c *C) {
	driver.FreeCloser()
}

func (ys *YigSuite) SetUpTest(c *C) {
}

func (ys *YigSuite) TearDownTest(c *C) {
}
