package sonyoda

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	log "github.com/sirupsen/logrus"

	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
)

//Sony-ODA only support standard(Tier-1)
/*func (mover *ODAMover) ChangeStorageClass(objKey *string, newClass *string, loca *BackendInfo) error {
	log.Infof("[odalifecycle] Change storage class of object[%s] to %s.", objKey, newClass)
	s3c := ODACred{ak: loca.Access, sk: loca.Security}
	creds := credentials.NewCredentials(&s3c)
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(loca.Region),
		Endpoint:    aws.String(loca.EndPoint),
		Credentials: creds,
	})
	if err != nil {
		log.Errorf("[odalifecycle] new session failed, err:%v\n", err)
		return handleODAErrors(err)
	}

	svc := s3.New(sess)
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(loca.BucketName),
		Key:        aws.String(*objKey),
		CopySource: aws.String(loca.BucketName + "/" + *objKey),
	}
	input.StorageClass = aws.String(*newClass)
	_, err = svc.CopyObject(input)
	if err != nil {
		log.Errorf("[odalifecycle] Change storage class of object[%s] to %s failed: %v.\n", objKey, newClass, err)
		e := handleODAErrors(err)
		return e
	}

	// TODO: How to make sure copy is complemented? Wait to see if the item got copied (example:svc.WaitUntilObjectExists)?

	return nil
}*/

func (mover *ODAMover) DeleteIncompleteMultipartUpload(objKey, uploadId string, loc *LocationInfo) error {
	log.Infof("[odalifecycle] Abort multipart upload[objkey:%s] for uploadId#%s.\n", objKey, uploadId)
	s3c := ODACred{ak: loc.Access, sk: loc.Security}
	creds := credentials.NewCredentials(&s3c)
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(loc.Region),
		Endpoint:    aws.String(loc.EndPoint),
		Credentials: creds,
	})
	if err != nil {
		log.Errorf("[odalifecycle] new session failed, err:%v\n", err)
		return handleODAErrors(err)
	}

	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(loc.BucketName),
		Key:      aws.String(objKey),
		UploadId: aws.String(uploadId),
	}

	svc := s3.New(sess)
	_, err = svc.AbortMultipartUpload(abortInput)
	e := handleODAErrors(err)
	if e == nil || e.Error() == DMERR_NoSuchUpload {
		log.Infof("[odalifecycle] abort multipart upload[objkey:%s, uploadid:%s] successfully.\n", objKey, uploadId)
		return nil
	} else {
		log.Infof("[odalifecycle] abort multipart upload[objkey:%s, uploadid:%s] failed, err:%v.\n", objKey, uploadId, err)
	}

	return e
}
