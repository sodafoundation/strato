package ceph

import (
	"bufio"
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"

	backendpb "github.com/opensds/multi-cloud/backend/proto"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	pb "github.com/opensds/multi-cloud/s3/proto"

	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/webrtcn/s3client"
	. "github.com/webrtcn/s3client"
)

type TestCephAdapter struct {
	backend *backendpb.BackendDetail
	session *s3client.Client
}

func TestInit(t *testing.T) {

	//actual
	backend := &backendpb.BackendDetail{
		Access:     "M20C8OSI4KTBECSE8ODZ",
		BucketName: "nilimabucket",
		Endpoint:   "http://192.168.1.192:7480",
		Id:         "qwsdfg123",
		Name:       "name",
		Region:     "hongkong",
		Security:   "eITJSnuq3JgPDspJhTo5Q4DPU3EczPHdHxiQI3rw",
		TenantId:   "ghgsadfhghshhsad",
	}

	// expected
	backend2 := &backendpb.BackendDetail{
		Access:     "M20C8OSI4KTBECSE8ODZ",
		BucketName: "nilimabucket",
		Endpoint:   "http://192.168.1.192:7480",
		Id:         "qwsdfg123",
		Name:       "name",
		Region:     "hongkong",
		Security:   "eITJSnuq3JgPDspJhTo5Q4DPU3EczPHdHxiQI3rw",
		TenantId:   "ghgsadfhghshhsad",
	}
	endpoint := backend.Endpoint
	AccessKeyID := backend.Access
	AccessKeySecret := backend.Security
	sess := s3client.NewClient(endpoint, AccessKeyID, AccessKeySecret)

	adap := TestCephAdapter{backend: backend, session: sess}

	storeAdapater := Init(backend2)
	assert.Equal(t, adap.backend, storeAdapater.backend, "Backend Doesn't match")

}

func TestCephAdapter_PUT(t *testing.T) {

	stream, _ := http.NewRequest(http.MethodPut, "/v1/s3/mybucket/Linuxcommand.jpg", nil)

	ctx := context.WithValue(stream.Context(), "operation", "upload")

	object2 := &pb.Object{
		ObjectKey:  "Linuxcommand.jpg",
		BucketName: "archanabucket",
		Size:       100}

	backend := &backendpb.BackendDetail{
		Access:     "M20C8OSI4KTBECSE8ODZ",
		BucketName: "archanabucket",
		Endpoint:   "http://192.168.1.192:7480",
		Id:         "qwsdfg123",
		Name:       "name",
		Region:     "hongkong",
		Security:   "eITJSnuq3JgPDspJhTo5Q4DPU3EczPHdHxiQI3rw",
		TenantId:   "ghgsadfhghshhsad",
	}
	storeAdapater := Init(backend)
	content := []byte("Did gyre and gimble in the wabe")
	co := bytes.NewReader(content)

	err2 := storeAdapater.PUT(co, object2, ctx)

	assert.Equal(t, S3Error{Code: 200, Description: ""}, err2, "Upload object to ceph failed")

}

func timeCost(start time.Time, method string) {
	_ = time.Since(start)
}

func hash(filename string) (md5String string, err error) {
	defer timeCost(time.Now(), "Hash")
	fi, err := os.Open(filename)
	if err != nil {
		return
	}
	defer fi.Close()
	reader := bufio.NewReader(fi)
	md5Ctx := md5.New()
	_, err = io.Copy(md5Ctx, reader)
	if err != nil {
		return
	}
	cipherStr := md5Ctx.Sum(nil)
	value := base64.StdEncoding.EncodeToString(cipherStr)
	return value, nil
}

func TestCephAdapter_DELETE(t *testing.T) {
	object := &pb.DeleteObjectInput{
		Key:    "LinuxCommand.jpg",
		Bucket: "archanabucket",
	}
	backend := &backendpb.BackendDetail{
		Access:     "M20C8OSI4KTBECSE8ODZ",
		BucketName: "archanabucket",
		Endpoint:   "http://192.168.1.192:7480",
		Id:         "qwsdfg123",
		Name:       "name",
		Region:     "hongkong",
		Security:   "eITJSnuq3JgPDspJhTo5Q4DPU3EczPHdHxiQI3rw",
		TenantId:   "ghgsadfhghshhsad",
	}

	storeAdapater := Init(backend)
	request, _ := http.NewRequest(http.MethodDelete, backend.Endpoint, nil)
	ctx := context.WithValue(request.Context(), "operation", "")
	err := storeAdapater.DELETE(object, ctx)
	assert.Equal(t, S3Error{Code: 200, Description: ""}, err, "Delete the object from ceph failed")

}

func TestCephAdapter_GET(t *testing.T) {
	var start int64
	var end int64
	backend := &backendpb.BackendDetail{
		Access:     "M20C8OSI4KTBECSE8ODZ",
		BucketName: "archanabucket",
		Endpoint:   "http://192.168.1.192:7480",
		Id:         "qwsdfg123",
		Name:       "name",
		Region:     "hongkong",
		Security:   "eITJSnuq3JgPDspJhTo5Q4DPU3EczPHdHxiQI3rw",
		TenantId:   "ghgsadfhghshhsad",
	}
	endpoint := backend.Endpoint
	AccessKeyID := backend.Access
	AccessKeySecret := backend.Security
	sess := s3client.NewClient(endpoint, AccessKeyID, AccessKeySecret)

	adap := TestCephAdapter{backend: backend, session: sess}

	bucket := adap.session.NewBucket()

	cephObject := bucket.NewObject(adap.backend.BucketName)
	object1 := &pb.Object{
		ObjectKey:  "Linuxcommand.jpg",
		BucketName: "archanabucket",
	}

	_, err := cephObject.Get(object1.ObjectKey, &GetObjectOption{})

	if err != nil {

	} else {

	}

	backend2 := &backendpb.BackendDetail{
		Access:     "M20C8OSI4KTBECSE8ODZ",
		BucketName: "archanabucket",
		Endpoint:   "http://192.168.1.192:7480",
		Id:         "qwsdfg123",
		Name:       "name",
		Region:     "hongkong",
		Security:   "eITJSnuq3JgPDspJhTo5Q4DPU3EczPHdHxiQI3rw",
		TenantId:   "ghgsadfhghshhsad",
	}

	object2 := &pb.Object{
		ObjectKey:  "Linuxcommand.jpg",
		BucketName: "archanabucket",
	}

	storeAdapater := Init(backend2)
	request, _ := http.NewRequest(http.MethodPut, backend.Endpoint, nil)
	ctx := context.WithValue(request.Context(), "operation", "download")
	_, err2 := storeAdapater.GET(object2, ctx, start, end)
	assert.Equal(t, S3Error{Code: 200, Description: ""}, err2, "Download failed")

}

func TestCephAdapter_InitMultipartUpload(t *testing.T) {
	backend := &backendpb.BackendDetail{
		Access:     "M20C8OSI4KTBECSE8ODZ",
		BucketName: "archanabucket",
		Endpoint:   "http://192.168.1.192:7480",
		Id:         "qwsdfg123",
		Name:       "name",
		Region:     "hongkong",
		Security:   "eITJSnuq3JgPDspJhTo5Q4DPU3EczPHdHxiQI3rw",
		TenantId:   "ghgsadfhghshhsad",
	}
	storeAdapater := Init(backend)
	stream, _ := http.NewRequest(http.MethodPut, "/v1/s3/mybucket/Linuxcommand.jpg", nil)

	ctx := context.WithValue(stream.Context(), "operation", "uploads")

	object2 := &pb.Object{
		ObjectKey:  "Linuxcommand.jpg",
		BucketName: "archanabucket",
	}
	_, err2 := storeAdapater.InitMultipartUpload(object2, ctx)

	assert.Equal(t, S3Error{Code: 200, Description: ""}, err2, "Init Multipart upload parts failed")
}

func TestCephAdapter_UploadPart(t *testing.T) {

	backend := &backendpb.BackendDetail{
		Access:     "M20C8OSI4KTBECSE8ODZ",
		BucketName: "archanabucket",
		Endpoint:   "http://192.168.1.192:7480",
		Id:         "qwsdfg123",
		Name:       "name",
		Region:     "hongkong",
		Security:   "eITJSnuq3JgPDspJhTo5Q4DPU3EczPHdHxiQI3rw",
		TenantId:   "ghgsadfhghshhsad",
	}

	storeAdapater := Init(backend)
	bucket := storeAdapater.session.NewBucket()
	object := &pb.Object{
		ObjectKey:  "Linuxcommand.jpg",
		BucketName: "archanabucket",
	}
	newObjectKey := object.BucketName + "/" + object.ObjectKey
	cephObject := bucket.NewObject(storeAdapater.backend.BucketName)
	uploader := cephObject.NewUploads(newObjectKey)
	multipartUpload := &pb.MultipartUpload{}

	res, err := uploader.Initiate(nil)

	if err != nil {

	} else {
		multipartUpload.Bucket = object.BucketName
		multipartUpload.Key = object.ObjectKey
		multipartUpload.UploadId = res.UploadID

	}
	content := []byte("Did gyre and gimble in the wabe")
	co := bytes.NewReader(content)

	stream, _ := http.NewRequest(http.MethodPut, "/v1/s3/mybucket/Linuxcommand.jpg", nil)

	ctx := context.WithValue(stream.Context(), "operation", "uploads")

	_, error := storeAdapater.UploadPart(co, multipartUpload, 1, 0, ctx)

	assert.Equal(t, S3Error{Code: 200, Description: ""}, error, "Upload parts to ceph failed")

}

func TestCephAdapter_CompleteMultipartUpload(t *testing.T) {
	//var completeParts *CompleteParts
	//objectSize, err :=getLength("E:/","Blog.mp4")
	backend := &backendpb.BackendDetail{
		Access:     "M20C8OSI4KTBECSE8ODZ",
		BucketName: "archanabucket",
		Endpoint:   "http://192.168.1.192:7480",
		Id:         "qwsdfg123",
		Name:       "name",
		Region:     "hongkong",
		Security:   "eITJSnuq3JgPDspJhTo5Q4DPU3EczPHdHxiQI3rw",
		TenantId:   "ghgsadfhghshhsad",
	}

	storeAdapater := Init(backend)
	bucket := storeAdapater.session.NewBucket()
	object := &pb.Object{
		ObjectKey:  "Blog.txt",
		BucketName: "archanabucket",
	}
	newObjectKey := object.BucketName + "/" + object.ObjectKey
	cephObject := bucket.NewObject(storeAdapater.backend.BucketName)
	uploader := cephObject.NewUploads(newObjectKey)
	multipartUpload := &pb.MultipartUpload{}

	res, err := uploader.Initiate(nil)

	if err != nil {

	} else {
		multipartUpload.Bucket = object.BucketName
		multipartUpload.Key = object.ObjectKey
		multipartUpload.UploadId = res.UploadID

	}
	content := []byte("Did gyre and gimble in the wabe")
	co := bytes.NewReader(content)
	stream, _ := http.NewRequest(http.MethodPut, "/v1/s3/mybucket/Blog.txt", nil)
	ctx := context.WithValue(stream.Context(), "operation", "uploads")
	result, _ := storeAdapater.UploadPart(co, multipartUpload, 1, 0, ctx)
	var Uploads UploadParts

	Uploads.ETag = result.ETag
	Uploads.PartNumber = result.PartNumber
	Uploads.Xmlns = result.Xmlns

	var parts model.Part
	parts.PartNumber = result.PartNumber
	parts.ETag = result.ETag
	UploadParts := &model.CompleteMultipartUpload{}
	UploadParts.Xmlns = Uploads.Xmlns
	UploadParts.Part = append(UploadParts.Part, parts)
	//*completeParts = append(*completeParts, *parts)
	_, errors := storeAdapater.CompleteMultipartUpload(multipartUpload, UploadParts, ctx)
	assert.Equal(t, S3Error{Code: 200, Description: ""}, errors, "Complete upload part failed")

}

func TestCephAdapter_AbortMultipartUpload(t *testing.T) {
	backend := &backendpb.BackendDetail{
		Access:     "M20C8OSI4KTBECSE8ODZ",
		BucketName: "archanabucket",
		Endpoint:   "http://192.168.1.192:7480",
		Id:         "qwsdfg123",
		Name:       "name",
		Region:     "hongkong",
		Security:   "eITJSnuq3JgPDspJhTo5Q4DPU3EczPHdHxiQI3rw",
		TenantId:   "ghgsadfhghshhsad",
	}

	storeAdapater := Init(backend)
	bucket := storeAdapater.session.NewBucket()
	object := &pb.Object{
		ObjectKey:  "Blog.txt",
		BucketName: "archanabucket",
	}
	newObjectKey := object.BucketName + "/" + object.ObjectKey
	cephObject := bucket.NewObject(storeAdapater.backend.BucketName)
	uploader := cephObject.NewUploads(newObjectKey)
	multipartUpload := &pb.MultipartUpload{}

	res, err := uploader.Initiate(nil)

	if err != nil {

	} else {

		multipartUpload.Bucket = object.BucketName
		multipartUpload.Key = object.ObjectKey
		multipartUpload.UploadId = res.UploadID

	}
	content := []byte("Did gyre and gimble in the wabe")
	co := bytes.NewReader(content)

	stream, _ := http.NewRequest(http.MethodPut, "/v1/s3/mybucket/Blog.txt", nil)
	ctx := context.WithValue(stream.Context(), "operation", "uploads")
	storeAdapater.UploadPart(co, multipartUpload, 1, 0, ctx)

	errors := storeAdapater.AbortMultipartUpload(multipartUpload, ctx)
	assert.Equal(t, S3Error{Code: 200, Description: ""}, errors, "Unable to Abort the multipart Upload")
}

func TestCephAdapter_ListParts(t *testing.T) {
	backend := &backendpb.BackendDetail{
		Access:     "M20C8OSI4KTBECSE8ODZ",
		BucketName: "archanabucket",
		Endpoint:   "http://192.168.1.192:7480",
		Id:         "qwsdfg123",
		Name:       "name",
		Region:     "hongkong",
		Security:   "eITJSnuq3JgPDspJhTo5Q4DPU3EczPHdHxiQI3rw",
		TenantId:   "ghgsadfhghshhsad",
	}

	storeAdapater := Init(backend)
	bucket := storeAdapater.session.NewBucket()
	object := &pb.Object{
		ObjectKey:  "Blog.txt",
		BucketName: "archanabucket",
	}
	newObjectKey := object.BucketName + "/" + object.ObjectKey
	cephObject := bucket.NewObject(storeAdapater.backend.BucketName)
	uploader := cephObject.NewUploads(newObjectKey)
	res, _ := uploader.Initiate(nil)
	stream, _ := http.NewRequest(http.MethodPut, "/v1/s3/mybucket/Linuxcommand.jpg", nil)

	ctx := context.WithValue(stream.Context(), "operation", "uploads")

	listParts := &pb.ListParts{}
	listParts.UploadId = res.UploadID
	listParts.Key = object.ObjectKey
	listParts.Bucket = object.BucketName
	_, error := storeAdapater.ListParts(listParts, ctx)

	assert.Equal(t, S3Error{Code: 200, Description: ""}, error, "Listing the upload parts failed")
}

func TestCephAdapter_GetObjectInfo(t *testing.T) {
	backend := &backendpb.BackendDetail{
		Access:     "M20C8OSI4KTBECSE8ODZ",
		BucketName: "archanabucket",
		Endpoint:   "http://192.168.1.192:7480",
		Id:         "qwsdfg123",
		Name:       "name",
		Region:     "hongkong",
		Security:   "eITJSnuq3JgPDspJhTo5Q4DPU3EczPHdHxiQI3rw",
		TenantId:   "ghgsadfhghshhsad",
	}
	endpoint := backend.Endpoint
	AccessKeyID := backend.Access
	AccessKeySecret := backend.Security
	sess := s3client.NewClient(endpoint, AccessKeyID, AccessKeySecret)

	adap := TestCephAdapter{backend: backend, session: sess}

	bucket := adap.session.NewBucket()

	cephObject := bucket.NewObject(adap.backend.BucketName)
	object1 := &pb.Object{
		ObjectKey:  "Linuxcommand.jpg",
		BucketName: "archanabucket",
	}

	cephObject.Get(object1.ObjectKey, &GetObjectOption{})

	backend2 := &backendpb.BackendDetail{
		Access:     "M20C8OSI4KTBECSE8ODZ",
		BucketName: "archanabucket",
		Endpoint:   "http://192.168.1.192:7480",
		Id:         "qwsdfg123",
		Name:       "name",
		Region:     "hongkong",
		Security:   "eITJSnuq3JgPDspJhTo5Q4DPU3EczPHdHxiQI3rw",
		TenantId:   "ghgsadfhghshhsad",
	}

	object2 := &pb.Object{
		ObjectKey:  "Linuxcommand.jpg",
		BucketName: "archanabucket",
	}

	storeAdapater := Init(backend2)
	request, _ := http.NewRequest(http.MethodPut, backend.Endpoint, nil)
	ctx := context.WithValue(request.Context(), "operation", "download")
	_, err2 := storeAdapater.GetObjectInfo(backend.BucketName, object2.ObjectKey, ctx)

	assert.Equal(t, S3Error{Code: 200, Description: ""}, err2, "Download failed")

}

type UploadParts struct {
	Xmlns      string `xml:"xmlns,attr"`
	PartNumber int64  `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}
