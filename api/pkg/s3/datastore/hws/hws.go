package hws

import (
	"github.com/micro/go-log"
	. "github.com/opensds/go-panda/s3/pkg/exception"
	pb "github.com/opensds/go-panda/s3/proto"
	"io"
	"obs"
)

type OBSAdapter struct {
}

func Init() *OBSAdapter {
	adap := &OBSAdapter{}
	return adap
}

func (ad *OBSAdapter) PUT(stream io.Reader, object *pb.Object) S3Error {
	endpoint := "obs.cn-north-1.myhwclouds.com"
	AccessKeyId := "4X7JQDFTCKUNWFBRYZVC"
	AccessKeySecret := "9hr0ekZgg6vZHulEekTVfWuu1lnPFvpVAJQNHXdn"
	client, err := obs.New(AccessKeyId, AccessKeySecret, endpoint)

	if err != nil {
		log.Logf("Access obs failed:%v", err)
		return S3Error{Code: 500, Description: "Access obs failed"}
	}
	input := &obs.PutObjectInput{}
	input.Bucket = "obs-wbtest"
	input.Key = object.BucketName + "\\" + object.ObjectKey
	input.Body = stream

	out, err := client.PutObject(input)

	if err != nil {
		log.Logf("Upload to obs failed:%v", err)
		return S3Error{Code: 500, Description: "Upload to obs failed"}
	}
	log.Logf("Upload %s to obs successfully.", out.VersionId)
	object.ServerVersionId = out.VersionId
	return NoError
}
