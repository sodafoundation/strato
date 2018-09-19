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
	"strconv"
	"errors"
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
		Key:aws.String(obj.Obj.ObjectKey),
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
		Key:aws.String(obj.Obj.ObjectKey),
	})

	if err != nil {
		log.Fatalf("download faild, err:%v\n", err)
	}else {
		log.Log("downlad succeed, bytes:%d\n", numBytes)
	}

	return numBytes, err
}

func DownloadRangeS3(objKey string, srcLoca *LocationInfo, buf []byte, start int64, end int64) (size int64, err error){
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
	/*getObjInput := s3.GetObjectInput{
		Bucket:aws.String("obs-test-123"),
		Key:aws.String("zsf-test"),
	}*/
	getObjInput := s3.GetObjectInput{
		Bucket:aws.String(srcLoca.BucketName),
		Key:aws.String(objKey),
	}
	strStart := strconv.FormatInt(start, 10)
	strEnd := strconv.FormatInt(end, 10)
	rg := "bytes=" + strStart + "-" + strEnd
	getObjInput.SetRange(rg)
	log.Logf("range:=%s\n", rg)
	numBytes, err := downLoader.Download(writer, &getObjInput)

	if err != nil {
		log.Fatalf("download faild, err:%v\n", err)
	}else {
		log.Logf("downlad succeed, bytes:%d\n", numBytes)
	}

	return numBytes,err
}

func MultiPartUploadInitS3(objKey string, destLoca *LocationInfo) (svc *s3.S3, uploadId string, err error) {
	s3c := s3Cred{ak:destLoca.Access, sk:destLoca.Security}
	creds := credentials.NewCredentials(&s3c)
	//cfg := aws.NewConfig().WithRegion("cn-north-1").WithCredentials(creds).WithEndpoint("obs.cn-north-1.myhwclouds.com")
	sess, err := session.NewSession(&aws.Config{
		Region:aws.String(destLoca.Region),
		Endpoint:aws.String(destLoca.EndPoint),
		Credentials:creds,
	})
	if err != nil {
		log.Logf("New session failed, err:%v\n", err)
		return nil,"",err
	}

	svc = s3.New(sess)
	multiUpInput := &s3.CreateMultipartUploadInput{
		Bucket:aws.String(destLoca.BucketName),
		Key:aws.String(objKey),
	}
	resp, err := svc.CreateMultipartUpload(multiUpInput)
	if err != nil {
		log.Fatalf("Init s3 multipart upload failed, err:%v\n", err)
		return nil,"", errors.New("Init s3 multipart upload failed.")
	}else {
		log.Logf("Init s3 multipart upload succeed, UploadId:%s\n", resp.UploadId)
	}

	return svc, *resp.UploadId, nil
}

func UploadPartS3(objKey string, destLoca *LocationInfo, svc *s3.S3, uploadId string, upBytes int64, buf []byte, partNumber int64)(*s3.CompletedPart, error) {
	tries := 1
	upPartInput := &s3.UploadPartInput{
		Body:bytes.NewReader(buf),
		Bucket:aws.String(destLoca.BucketName),
		Key:aws.String(objKey),
		PartNumber:aws.Int64(partNumber),
		UploadId:aws.String(uploadId),
		ContentLength:aws.Int64(upBytes),
	}

	log.Logf("len(buf)=%d, upBytes=%d,uploadId=%s,partNumber=%d\n", len(buf), upBytes, uploadId,partNumber)

	for tries <= 3 {
		upRes, err := svc.UploadPart(upPartInput)
		if err != nil {
			if tries == 3 {
				log.Fatalf("Upload part to aws failed. err:%v\n", err)
				return nil, err
			}
			log.Logf("Retrying to upload part#%d\n", partNumber)
		}else {
			log.Logf("Uploaded part #%d\n", partNumber)
			return &s3.CompletedPart{
				ETag:upRes.ETag,
				PartNumber:aws.Int64(partNumber),
			},nil
		}
	}

	return nil, errors.New("Internal error")
}

func AbortMultipartUpload(objKey string, destLoca *LocationInfo, svc *s3.S3, uploadId string) error {
	log.Logf("Aborting multipart upload for uploadId#%s.\n", uploadId)
	abortInput := &s3.AbortMultipartUploadInput{
		Bucket: aws.String(destLoca.BucketName),
		Key:aws.String(objKey),
		UploadId:aws.String(uploadId),
	}

	_,err := svc.AbortMultipartUpload(abortInput)
	return err
}

func CompleteMultipartUploadS3(objKey string, destLoca *LocationInfo, svc *s3.S3, uploadId string, completeParts []*s3.CompletedPart) error {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:aws.String(destLoca.BucketName),
		Key:aws.String(objKey),
		UploadId:aws.String(uploadId),
		MultipartUpload:&s3.CompletedMultipartUpload{
			Parts:completeParts,
		},
	}

	rsp, err := svc.CompleteMultipartUpload(completeInput)
	if err != nil {
		log.Logf("completeMultipartUploadS3 failed, err:%v\n", err)
	}else {
		log.Logf("completeMultipartUploadS3 successfully, rsp:%v\n", rsp)
	}

	return err
}

func DeleteObj(obj *SourceOject, loca *LocationInfo) error {
	s3c := s3Cred{ak:loca.Access, sk:loca.Security}
	creds := credentials.NewCredentials(&s3c)
	sess, err := session.NewSession(&aws.Config{
		Region:aws.String(loca.Region),
		Endpoint:aws.String(loca.EndPoint),
		Credentials:creds,
	})
	if err != nil {
		log.Fatalf("New session failed, err:%v\n", err)
		return err
	}

	svc := s3.New(sess)
	_, err = svc.DeleteObject(&s3.DeleteObjectInput{Bucket: aws.String(loca.BucketName), Key: aws.String(obj.Obj.ObjectKey)})
	if err != nil {
		log.Logf("Unable to delete object %q from bucket %q, %v", obj, loca.BucketName, err)
	}

	err = svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(loca.BucketName),
		Key:    aws.String(obj.Obj.ObjectKey),
	})
	if err != nil {
		log.Logf("Error occurred while waiting for object %q to be deleted, %v", obj)
	}

	log.Logf("Object %q successfully deleted\n", obj)
	return err
}

