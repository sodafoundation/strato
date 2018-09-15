package s3mover

import (
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"bytes"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3"
	. "github.com/opensds/go-panda/datamover/pkg/utils"
	"github.com/micro/go-log"
)

type s3Cred struct {
	ak string
	sk string
}

func (myc *s3Cred) Retrieve() (credentials.Value,error){
	cred := credentials.Value{AccessKeyID:myc.ak, SecretAccessKey:myc.sk}
	return cred,nil
}

func (myc *s3Cred) IsExpired() bool {
	return false
}

func UploadS3Obj (obj *SourceOject, destLoca *LocationInfo, buf []byte) error {
	//s3c := s3Cred{ak:"4X7JQDFTCKUNWFBRYZVC", sk:"9hr0ekZgg6vZHulEekTVfWuu1lnPFvpVAJQNHXdn"}
	s3c := s3Cred{ak:destLoca.Access, sk:destLoca.Security}
	creds := credentials.NewCredentials(&s3c)
	/*sess, err := session.NewSession(&aws.Config{
		Region:aws.String("cn-north-1"),
		Endpoint:aws.String("obs.cn-north-1.myhwclouds.com"),
		Credentials:creds,
	})*/
	sess, err := session.NewSession(&aws.Config{
		Region:aws.String(destLoca.Region),
		Endpoint:aws.String(destLoca.EndPoint),
		Credentials:creds,
	})
	if err != nil {
		log.Fatalf("New session failed, err:%v\n", err)
		return err
	}

	reader := bytes.NewReader(buf)
	uploader := s3manager.NewUploader(sess)
	/*_,err = uploader.Upload(&s3manager.UploadInput{
		Bucket:aws.String("obs-ee0e"),
		Key:aws.String("mykey"),
		Body:reader,
	})*/
	_,err = uploader.Upload(&s3manager.UploadInput{
		Bucket:aws.String(destLoca.BucketName),
		Key:aws.String(obj.ObjKey),
		Body:reader,
	})
	if err != nil {
		log.Fatalf("upload failed, err:%v\n", err)
	} else {
		log.Log("upload succeed.")
	}

	return err
}

func DownloadS3Obj(obj *SourceOject, srcLoca *LocationInfo, buf []byte) (size int64, err error) {

	//s3c := s3Cred{ak:"4X7JQDFTCKUNWFBRYZVC", sk:"9hr0ekZgg6vZHulEekTVfWuu1lnPFvpVAJQNHXdn"}
	s3c := s3Cred{ak:srcLoca.Access, sk:srcLoca.Security}
	creds := credentials.NewCredentials(&s3c)
	/*sess, err := session.NewSession(&aws.Config{
		Region:aws.String("cn-north-1"),
		Endpoint:aws.String("obs.cn-north-1.myhwclouds.com"),
		Credentials:creds,
	})*/
	sess, err := session.NewSession(&aws.Config{
		Region:aws.String(srcLoca.Region),
		Endpoint:aws.String(srcLoca.EndPoint),
		Credentials:creds,
	})
	if err != nil {
		log.Fatalf("New session failed, err:%v\n", err)
		return 0, err
	}

	writer := aws.NewWriteAtBuffer(buf)
	downLoader := s3manager.NewDownloader(sess)
	/*numBytes, err := downLoader.Download(writer, &s3.GetObjectInput{
		Bucket:aws.String("obs-test-123"),
		Key:aws.String("objectkey"),
	})*/
	numBytes, err := downLoader.Download(writer, &s3.GetObjectInput{
		Bucket:aws.String(srcLoca.BucketName),
		Key:aws.String(obj.ObjKey),
	})

	if err != nil {
		log.Fatalf("download faild, err:%v\n", err)
	}else {
		log.Log("downlad succeed, bytes:%d\n", numBytes)
	}

	return numBytes, err
}
