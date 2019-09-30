package tests

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"

	"github.com/opensds/multi-cloud/backend/pkg/utils/constants"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	dscommon "github.com/opensds/multi-cloud/s3/pkg/datastore/common"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/driver"
	pb "github.com/opensds/multi-cloud/s3/proto"
	. "gopkg.in/check.v1"
)

func (ys *YigSuite) TestPutObjectSucceed(c *C) {
	detail := &backendpb.BackendDetail{
		Endpoint: "default",
	}

	yig, err := driver.CreateStorageDriver(constants.BackendTypeYIGS3, detail)
	c.Assert(err, Equals, nil)
	c.Assert(yig, Not(Equals), nil)

	// test small file put.
	len := 64 * 1024
	body := RandBytes(len)
	bodyReader := bytes.NewReader(body)
	rawMd5 := md5.Sum(body)
	bodyMd5 := hex.EncodeToString(rawMd5[:])
	ctx := context.Background()
	ctx = context.WithValue(ctx, dscommon.CONTEXT_KEY_SIZE, int64(len))
	ctx = context.WithValue(ctx, dscommon.CONTEXT_KEY_MD5, bodyMd5)
	obj := &pb.Object{
		ObjectKey:  "t1",
		BucketName: "b1",
	}
	result, err := yig.Put(ctx, bodyReader, obj)
	c.Assert(err, Equals, nil)
	c.Assert(result.Written, Equals, int64(len))
	c.Assert(result.ObjectId != "", Equals, true)
	c.Assert(result.Etag == bodyMd5, Equals, true)
}
