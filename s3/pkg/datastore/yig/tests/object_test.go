package tests

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"sync"
	"time"

	"github.com/opensds/multi-cloud/backend/pkg/utils/constants"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	dscommon "github.com/opensds/multi-cloud/s3/pkg/datastore/common"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/driver"
	"github.com/opensds/multi-cloud/s3/pkg/model"
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

	// Get the object
	obj.StorageMeta = result.Meta
	obj.ObjectId = result.ObjectId
	reader, err := yig.Get(ctx, obj, 0, int64(len-1))
	c.Assert(err, Equals, nil)
	c.Assert(reader, Not(Equals), nil)
	defer reader.Close()
	copyTarget := &pb.Object{
		ObjectKey:  "t2",
		BucketName: "b1",
		Size:       int64(len),
		Etag:       result.Etag,
	}
	// Copy the object
	result, err = yig.Copy(ctx, reader, copyTarget)
	c.Assert(err, Equals, nil)
	c.Assert(result.Written, Equals, int64(len))
	c.Assert(result.ObjectId != "", Equals, true)
	c.Assert(result.Etag == bodyMd5, Equals, true)

	// Get the copied object
	copyTarget.StorageMeta = result.Meta
	copyTarget.ObjectId = result.ObjectId
	reader2, err := yig.Get(ctx, copyTarget, 0, int64(len-1))
	c.Assert(err, Equals, nil)
	c.Assert(reader2, Not(Equals), nil)
	defer reader2.Close()
	readBuf := make([]byte, len)
	n, err := reader2.Read(readBuf)
	c.Assert(err, Equals, nil)
	c.Assert(n, Equals, len)
	readRawMd5 := md5.Sum(readBuf)
	c.Assert(rawMd5 == readRawMd5, Equals, true)

	// delete the object
	objDelInput := &pb.DeleteObjectInput{
		ObjectId:    obj.ObjectId,
		StorageMeta: obj.StorageMeta,
	}
	err = yig.Delete(ctx, objDelInput)
	c.Assert(err, Equals, nil)
	time.Sleep(10 * time.Second)
	reader, err = yig.Get(ctx, obj, 0, int64(len-1))
	c.Assert(err, Equals, nil)
	c.Assert(reader, Not(Equals), nil)
	n, err = reader.Read(readBuf[:])
	c.Assert(n, Equals, 0)
	c.Assert(err, Not(Equals), nil)
	// delete the copied object
	objDelInput = &pb.DeleteObjectInput{
		ObjectId:    copyTarget.ObjectId,
		StorageMeta: copyTarget.StorageMeta,
	}
	err = yig.Delete(ctx, objDelInput)
	c.Assert(err, Equals, nil)
	time.Sleep(10 * time.Second)
	reader, err = yig.Get(ctx, copyTarget, 0, int64(len-1))
	c.Assert(err, Equals, nil)
	c.Assert(reader, Not(Equals), nil)
	n, err = reader.Read(readBuf[:])
	c.Assert(n, Equals, 0)
	c.Assert(err, Not(Equals), nil)
}

type ReaderElem struct {
	Len    int
	Reader *bytes.Reader
	Md5    string
	Result *model.UploadPartResult
	Err    error
}

func (ys *YigSuite) TestMultipartUploadSucceed(c *C) {
	detail := &backendpb.BackendDetail{
		Endpoint: "default",
	}

	yig, err := driver.CreateStorageDriver(constants.BackendTypeYIGS3, detail)
	c.Assert(err, Equals, nil)
	c.Assert(yig, Not(Equals), nil)

	var readerElems []*ReaderElem
	// 1st part
	elem := &ReaderElem{
		Len: 5 * 1024 * 1024,
	}
	body := RandBytes(elem.Len)
	elem.Reader = bytes.NewReader(body)
	rawMd5 := md5.Sum(body)
	elem.Md5 = hex.EncodeToString(rawMd5[:])
	readerElems = append(readerElems, elem)
	// 2st part
	elem = &ReaderElem{
		Len: 6 * 1024 * 1024,
	}
	body = RandBytes(elem.Len)
	elem.Reader = bytes.NewReader(body)
	rawMd5 = md5.Sum(body)
	elem.Md5 = hex.EncodeToString(rawMd5[:])
	readerElems = append(readerElems, elem)
	// 3st part
	elem = &ReaderElem{
		Len: 7 * 1024 * 1024,
	}
	body = RandBytes(elem.Len)
	elem.Reader = bytes.NewReader(body)
	rawMd5 = md5.Sum(body)
	elem.Md5 = hex.EncodeToString(rawMd5[:])
	readerElems = append(readerElems, elem)
	// final part
	elem = &ReaderElem{
		Len: 64 * 1024,
	}
	body = RandBytes(elem.Len)
	elem.Reader = bytes.NewReader(body)
	rawMd5 = md5.Sum(body)
	elem.Md5 = hex.EncodeToString(rawMd5[:])
	readerElems = append(readerElems, elem)

	obj := &pb.Object{
		ObjectKey:  "m1",
		BucketName: "b1",
	}

	mu, err := yig.InitMultipartUpload(context.Background(), obj)
	c.Assert(err, Equals, nil)
	c.Assert(mu, Not(Equals), nil)
	c.Assert(mu.UploadId != "", Equals, true)

	obj.ObjectId = mu.UploadId

	wg := sync.WaitGroup{}

	loopCount := len(readerElems)
	for i := 0; i < loopCount; i++ {
		wg.Add(1)
		go func(num int, elem *ReaderElem) {
			ctx := context.Background()
			ctx = context.WithValue(ctx, dscommon.CONTEXT_KEY_MD5, elem.Md5)
			elem.Result, elem.Err = yig.UploadPart(ctx, elem.Reader, mu, int64(num), int64(elem.Len))
			wg.Done()
		}(i+1, readerElems[i])
	}

	wg.Wait()

	completes := &model.CompleteMultipartUpload{}
	for i := 0; i < loopCount; i++ {
		c.Assert(readerElems[i].Err, Equals, nil)
		c.Assert(readerElems[i].Result, Not(Equals), nil)
		c.Assert(int64(i+1), Equals, readerElems[i].Result.PartNumber)
		c.Assert(readerElems[i].Md5, Equals, readerElems[i].Result.ETag)
		part := model.Part{
			PartNumber: readerElems[i].Result.PartNumber,
			ETag:       readerElems[i].Result.ETag,
		}
		completes.Parts = append(completes.Parts, part)
	}

	completeResult, err := yig.CompleteMultipartUpload(context.Background(), mu, completes)
	c.Assert(err, Equals, nil)
	c.Assert(completeResult, Not(Equals), nil)
	c.Assert(completeResult.ETag != "", Equals, true)

	// verify get object
	start := int64(0)
	for i := 0; i < loopCount; i++ {
		objReader, err := yig.Get(context.Background(), obj, start, start+int64(readerElems[i].Len)-1)
		c.Assert(err, Equals, nil)
		c.Assert(objReader, Not(Equals), nil)
		start += int64(readerElems[i].Len)
		buf := make([]byte, readerElems[i].Len)
		n, err := objReader.Read(buf)
		c.Assert(err, Equals, nil)
		c.Assert(n, Equals, readerElems[i].Len)
		err = objReader.Close()
		c.Assert(err, Equals, nil)

		oriMd5 := md5.Sum(buf)
		resultMd5 := hex.EncodeToString(oriMd5[:])
		c.Assert(resultMd5 == readerElems[i].Md5, Equals, true)
	}

	objReader, err := yig.Get(context.Background(), obj, 0, int64(512<<20))
	c.Assert(err, Equals, nil)
	c.Assert(objReader, Not(Equals), nil)
	defer objReader.Close()
	total := 0
	for {
		toread := 5 << 20
		buf := make([]byte, toread)
		n, err := objReader.Read(buf)
		c.Assert(err, Equals, nil)
		total += n
		if n < toread {
			break
		}
	}
	oriTotal := 0
	for i := 0; i < loopCount; i++ {
		oriTotal += readerElems[i].Len
	}
	c.Assert(total, Equals, oriTotal)
	buf := make([]byte, 5<<20)
	n, err := objReader.Read(buf)
	c.Assert(n, Equals, 0)
	c.Assert(err, Equals, io.EOF)
	// list the parts of multipart uploaded object.
	listPartsReq := &pb.ListParts{
		Bucket:   obj.BucketName,
		Key:      obj.ObjectKey,
		UploadId: mu.UploadId,
	}

	listPartsResp, err := yig.ListParts(context.Background(), listPartsReq)
	c.Assert(err, Equals, nil)
	c.Assert(listPartsResp, Not(Equals), nil)
	c.Assert(len(listPartsResp.Parts), Equals, len(readerElems))
	// delete the multipart uploaded object.
	objDelInput := &pb.DeleteObjectInput{
		ObjectId: obj.ObjectId,
	}
	err = yig.Delete(context.Background(), objDelInput)
	c.Assert(err, Equals, nil)
	time.Sleep(10 * time.Second)
	objReader, err = yig.Get(context.Background(), obj, 0, int64(1<<30))
	c.Assert(err, Equals, nil)
	c.Assert(objReader, Not(Equals), nil)
	n, err = objReader.Read(buf[:])
	c.Assert(n, Equals, 0)
	c.Assert(err, Not(Equals), nil)
}

func (ys *YigSuite) TestMultipartUploadSinglePartSucceed(c *C) {
	detail := &backendpb.BackendDetail{
		Endpoint: "default",
	}

	yig, err := driver.CreateStorageDriver(constants.BackendTypeYIGS3, detail)
	c.Assert(err, Equals, nil)
	c.Assert(yig, Not(Equals), nil)

	// final part
	elem := &ReaderElem{
		Len: 64 * 1024,
	}
	body := RandBytes(elem.Len)
	elem.Reader = bytes.NewReader(body)
	rawMd5 := md5.Sum(body)
	elem.Md5 = hex.EncodeToString(rawMd5[:])

	obj := &pb.Object{
		ObjectKey:  "m2",
		BucketName: "b2",
	}

	mu, err := yig.InitMultipartUpload(context.Background(), obj)
	c.Assert(err, Equals, nil)
	c.Assert(mu, Not(Equals), nil)
	c.Assert(mu.UploadId != "", Equals, true)

	obj.ObjectId = mu.UploadId

	ctx := context.Background()
	ctx = context.WithValue(ctx, dscommon.CONTEXT_KEY_MD5, elem.Md5)
	elem.Result, elem.Err = yig.UploadPart(ctx, elem.Reader, mu, int64(1), int64(elem.Len))

	completes := &model.CompleteMultipartUpload{}
	part := model.Part{
		PartNumber: elem.Result.PartNumber,
		ETag:       elem.Result.ETag,
	}
	completes.Parts = append(completes.Parts, part)

	completeResult, err := yig.CompleteMultipartUpload(context.Background(), mu, completes)
	c.Assert(err, Equals, nil)
	c.Assert(completeResult, Not(Equals), nil)
	c.Assert(completeResult.ETag != "", Equals, true)

	// verify get object
	objReader, err := yig.Get(context.Background(), obj, 0, int64(5<<20))
	c.Assert(err, Equals, nil)
	c.Assert(objReader, Not(Equals), nil)
	buf := make([]byte, elem.Len)
	n, err := objReader.Read(buf)
	c.Assert(err, Equals, nil)
	c.Assert(n, Equals, elem.Len)
	oriMd5 := md5.Sum(buf)
	resultMd5 := hex.EncodeToString(oriMd5[:])
	c.Assert(resultMd5 == elem.Md5, Equals, true)

	n, err = objReader.Read(buf[:])
	c.Assert(n, Equals, 0)
	c.Assert(err, Equals, io.EOF)
	err = objReader.Close()
	c.Assert(err, Equals, nil)

	// delete the multipart uploaded object.
	objDelInput := &pb.DeleteObjectInput{
		ObjectId: obj.ObjectId,
	}
	err = yig.Delete(context.Background(), objDelInput)
	c.Assert(err, Equals, nil)
	time.Sleep(10 * time.Second)
	objReader, err = yig.Get(context.Background(), obj, 0, int64(elem.Len-1))
	c.Assert(err, Equals, nil)
	c.Assert(objReader, Not(Equals), nil)
	n, err = objReader.Read(buf[:])
	c.Assert(n, Equals, 0)
	c.Assert(err, Not(Equals), nil)
}
