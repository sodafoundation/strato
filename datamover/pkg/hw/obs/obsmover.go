package obsmover

import (
	"github.com/hw/obs"
	"github.com/micro/go-log"
	. "github.com/opensds/go-panda/datamover/pkg/utils"
	"io"
	"bytes"
	"fmt"
)

func DownloadHwObsObj(obj *SourceOject, srcLoca *LocationInfo, buf []byte) (size int64, err error){
	input := &obs.GetObjectInput{}
	input.Bucket = srcLoca.BucketName
	input.Key = obj.ObjKey
	obsClient,err := obs.New(srcLoca.Access, srcLoca.Security, srcLoca.EndPoint)
	if err != nil {
		return 0,err
	}

	output, err := obsClient.GetObject(input)
	if err == nil {
		size = 0
		defer output.Body.Close()
		log.Logf("StorageClass:%s, ETag:%s, ContentType:%s, ContentLength:%d, LastModified:%s\n",
			output.StorageClass, output.ETag, output.ContentType, output.ContentLength, output.LastModified)
		var readErr error
		var readCount int = 0
		// read object
		for {
			s := buf[size:]
			readCount, readErr = output.Body.Read(s)
			//log.Logf("readCount=%d, readErr=%v\n", readCount, readErr)
			if readCount > 0 {
				size += int64(readCount)
			}
			if readErr != nil {
				log.Logf("readErr=%v\n", readErr)
				break
			}
		}
		if readErr == io.EOF {
			readErr = nil
		}
		return size,readErr
	} else if obsError, ok := err.(obs.ObsError); ok {
		log.Logf("Code:%s\n", obsError.Code)
		log.Logf("Message:%s\n", obsError.Message)
		return 0,err
	}
	return
}

func UploadHwObsObj(obj *SourceOject, destLoca *LocationInfo, buf []byte) error {
	input := &obs.PutObjectInput{}
	input.Bucket = destLoca.BucketName
	input.Key = obj.ObjKey
	obsClient,err := obs.New(destLoca.Access, destLoca.Security, destLoca.EndPoint)
	if err != nil {
		return err
	}
	input.Body = bytes.NewReader(buf)
	output, err := obsClient.PutObject(input)
	if err != nil {
		fmt.Printf("err: %v\n", err)
	}else {
		fmt.Printf("RequestId:%s, ETag:%s\n", output.RequestId, output.ETag)
	}

	return err
}
