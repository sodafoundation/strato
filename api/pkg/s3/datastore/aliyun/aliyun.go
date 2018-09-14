package aliyun

import (
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/micro/go-log"
	. "github.com/opensds/go-panda/s3/pkg/exception"
	pb "github.com/opensds/go-panda/s3/proto"
	"io"
)

type AliyunAdapter struct {
}

func Init() *AliyunAdapter {
	adap := &AliyunAdapter{}
	return adap
}

func (ad *AliyunAdapter) PUT(stream io.Reader, object *pb.Object) S3Error {
	aliyunEndpoint := "oss-cn-beijing.aliyuncs.com"
	AccessKeyId := "LTAIyCcP4NYqQESb"
	AccessKeySecret := "zDLE1lo8IziJfSOpg1NZnuqlZPXnj4"
	client, err := oss.New(aliyunEndpoint, AccessKeyId, AccessKeySecret)
	if err != nil {
		log.Logf("Access aliyun failed:%v", err)
		return S3Error{Code: 500, Description: "Access aliyun failed"}
	}

	bucket, err := client.Bucket("testwb01")
	if err != nil {
		log.Logf("Access bucket failed:%v", err)
		return S3Error{Code: 500, Description: "Access bucket failed"}
	}

	newObjectKey := object.BucketName + "\\" + object.ObjectKey

	err = bucket.PutObject(newObjectKey, stream)

	if err != nil {
		log.Logf("Upload to aliyun failed:%v", err)
		return S3Error{Code: 500, Description: "Upload to aliyun failed"}
	}
	return NoError
}
