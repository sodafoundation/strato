package tests

import (
	"testing"

	_ "github.com/opensds/multi-cloud/s3/pkg/datastore"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/driver"
	. "gopkg.in/check.v1"
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
