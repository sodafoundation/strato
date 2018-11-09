package azure

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/micro/go-log"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	pb "github.com/opensds/multi-cloud/s3/proto"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"
)

//TryTimeout indicates the maximum time allowed for any single try of an HTTP request.
var MaxTimeForSingleHttpRequest = 50 * time.Minute

type AzureAdapter struct {
	backend      *backendpb.BackendDetail
	containerURL azblob.ContainerURL
}

func Init(backend *backendpb.BackendDetail) *AzureAdapter {
	endpoint := backend.Endpoint
	AccessKeyID := backend.Access
	AccessKeySecret := backend.Security
	ad := AzureAdapter{}
	containerURL, err := ad.createContainerURL(endpoint, AccessKeyID, AccessKeySecret)
	if err != nil {
		log.Logf("AzureAdapter Init container URL faild:%v\n", err)
		return nil
	}
	adap := &AzureAdapter{backend: backend, containerURL: containerURL}
	log.Log("AzureAdapter Init succeed, container URL:", containerURL.String())
	return adap
}

func (ad *AzureAdapter) createContainerURL(endpoint string, acountName string, accountKey string) (azblob.ContainerURL,
	error) {
	credential, err := azblob.NewSharedKeyCredential(acountName, accountKey)

	if err != nil {
		log.Logf("Create credential failed, err:%v\n", err)
		return azblob.ContainerURL{}, err
	}

	//create containerURL
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			TryTimeout: MaxTimeForSingleHttpRequest,
		},
	})
	URL, _ := url.Parse(endpoint)

	return azblob.NewContainerURL(*URL, p), nil
}

func (ad *AzureAdapter) PUT(stream io.Reader, object *pb.Object, ctx context.Context) S3Error {
	log.Logf("PUT  method receive request")
	bucket := ad.backend.BucketName
	log.Logf("bucket is %v\n", bucket)
	newObjectKey := object.BucketName + "/" + object.ObjectKey
	blobURL := ad.containerURL.NewBlockBlobURL(newObjectKey)
	log.Logf("blobURL is %v\n", blobURL)
	bytess, _ := ioutil.ReadAll(stream)
	uploadResp, err := blobURL.Upload(ctx, bytes.NewReader(bytess), azblob.BlobHTTPHeaders{}, nil,
		azblob.BlobAccessConditions{})
	if err != nil {
		log.Logf("[AzureAdapter] Upload faild,err = %v\n", err)
		return S3Error{Code: 500, Description: "Upload to azure failed"}
	} else {
		object.LastModified = time.Now().String()[:19]
		log.Logf("LastModified is:%v\n", object.LastModified)
	}

	if uploadResp.StatusCode() != http.StatusCreated {
		log.Logf("[AzureAdapter] Upload StatusCode:%d\n", uploadResp.StatusCode())
		return S3Error{Code: 500, Description: "azure failed"}
	}

	log.Log("[AzureAdapter] Upload successfully.")
	return NoError
}
func (ad *AzureAdapter) GET(object *pb.Object, context context.Context) (io.ReadCloser, S3Error) {
	bucket := ad.backend.BucketName
	log.Logf("bucket is %v\n", bucket)
	newObjectKey := object.BucketName + "/" + object.ObjectKey
	blobURL := ad.containerURL.NewBlobURL(newObjectKey)
	log.Logf("blobURL is %v\n", blobURL)
	log.Logf("object.Size is %v \n", object.Size)
	len := object.Size
	var buf = make([]byte, len)
	err := azblob.DownloadBlobToBuffer(context, blobURL, 0, 0, buf, azblob.DownloadFromBlobOptions{})
	if err != nil {
		log.Logf("[AzureAdapter] Download failed:%v\n", err)
		return nil, S3Error{Code: 500, Description: "Download failed"}
	}
	body := bytes.NewReader(buf)
	log.Logf("Download succeed, body:%v\n", *body)
	ioReaderClose := ioutil.NopCloser(body)
	log.Logf("Download succeed, ioReaderClose:%v\n", ioReaderClose)
	return ioReaderClose, NoError
}
func (ad *AzureAdapter) DELETE(object *pb.DeleteObjectInput, ctx context.Context) S3Error {
	bucket := ad.backend.BucketName
	log.Logf("bucket is %v\n", bucket)
	newObjectKey := object.Bucket + "/" + object.Key
	blobURL := ad.containerURL.NewBlockBlobURL(newObjectKey)
	log.Logf("blobURL is %v\n", blobURL)
	delRsp, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{})
	log.Logf("blobURL=%v,err=%v\n", blobURL, err)
	if err != nil {
		log.Logf("[AzureAdapter] Delete failed:%v\n", err)
		return S3Error{Code: 500, Description: "Delete failed"}
	}

	if delRsp.StatusCode() != http.StatusOK && delRsp.StatusCode() != http.StatusAccepted {
		log.Logf("[AzureAdapter] Delete failed, status code:%d\n", delRsp.StatusCode())
		return S3Error{Code: 500, Description: "Delete failed"}
	}
	return NoError
}
func (ad *AzureAdapter) GetObjectInfo(bucketName string, key string, context context.Context) (*pb.Object, S3Error) {
	object := pb.Object{}
	object.BucketName = bucketName
	object.ObjectKey = key
	return &object, NoError
}

func (ad *AzureAdapter) InitMultipartUpload(object *pb.Object, context context.Context) (*pb.MultipartUpload, S3Error) {
	bucket := ad.backend.BucketName
	log.Logf("bucket is %v\n", bucket)
	multipartUpload := &pb.MultipartUpload{}
	multipartUpload.Key = object.ObjectKey
	multipartUpload.Bucket = object.BucketName
	multipartUpload.UploadId = object.ObjectKey
	return multipartUpload, NoError
}

func (ad *AzureAdapter) Int64ToBase64(blockID int64) string {
	buf := (&[8]byte{})[:]
	binary.LittleEndian.PutUint64(buf, uint64(blockID))
	return ad.BinaryToBase64(buf)
}

func (ad *AzureAdapter) BinaryToBase64(binaryID []byte) string {
	return base64.StdEncoding.EncodeToString(binaryID)
}

func (ad *AzureAdapter) Base64ToInt64(base64ID string) int64 {
	bin, _ := base64.StdEncoding.DecodeString(base64ID)
	return int64(binary.LittleEndian.Uint64(bin))
}

func (ad *AzureAdapter) UploadPart(stream io.Reader, multipartUpload *pb.MultipartUpload, partNumber int64, upBytes int64, context context.Context) (*model.UploadPartResult, S3Error) {
	bucket := ad.backend.BucketName
	log.Logf("bucket is %v\n", bucket)
	newObjectKey := multipartUpload.Bucket + "/" + multipartUpload.Key
	blobURL := ad.containerURL.NewBlockBlobURL(newObjectKey)
	base64ID := ad.Int64ToBase64(partNumber)
	bytess, _ := ioutil.ReadAll(stream)
	_, err := blobURL.StageBlock(context, base64ID, bytes.NewReader(bytess), azblob.LeaseAccessConditions{}, nil)
	log.Logf("err is %v\n", err)
	if err != nil {
		log.Logf("[AzureAdapter] Stage block[#%d,base64ID:%s] failed:%v\n", partNumber, base64ID, err)
		return nil, S3Error{Code: 500, Description: "Delete failed"}
	}
	log.Logf("[AzureAdapter] Stage block[#%d,base64ID:%s] succeed.\n", partNumber, base64ID)
	result := &model.UploadPartResult{PartNumber: partNumber, ETag: newObjectKey}
	return result, NoError
}
func (ad *AzureAdapter) CompleteMultipartUpload(
	multipartUpload *pb.MultipartUpload,
	completeUpload *model.CompleteMultipartUpload,
	context context.Context) (*model.CompleteMultipartUploadResult, S3Error) {
	bucket := ad.backend.BucketName
	result := model.CompleteMultipartUploadResult{}

	log.Logf("bucket is %v\n", bucket)
	result.Bucket = multipartUpload.Bucket
	result.Key = multipartUpload.Key
	result.Location = ad.backend.Name
	newObjectKey := multipartUpload.Bucket + "/" + multipartUpload.Key
	log.Logf("newObjectKey is %v\n", newObjectKey)
	blobURL := ad.containerURL.NewBlockBlobURL(newObjectKey)
	var completeParts []string
	for _, p := range completeUpload.Part {
		base64ID := ad.Int64ToBase64(p.PartNumber)
		completeParts = append(completeParts, base64ID)
	}
	_, err := blobURL.CommitBlockList(context, completeParts, azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{})
	log.Logf("err is %v\n", err)
	if err != nil {
		log.Logf("[AzureAdapter] Commit blocks faild:%v\n", err)
		return nil, S3Error{Code: 500, Description: err.Error()}
	} else {
		log.Logf("[AzureAdapter] Commit blocks succeed:\n")
		return &result, NoError
	}
	return nil, NoError
}
func (ad *AzureAdapter) AbortMultipartUpload(multipartUpload *pb.MultipartUpload, context context.Context) S3Error {
	bucket := ad.backend.BucketName
	log.Logf("No need to abort multipart upload[objkey:%s].\n", bucket)
	return NoError
}
func (ad *AzureAdapter) ListParts(listParts *pb.ListParts, context context.Context) (*model.ListPartsOutput, S3Error) {
	return nil, NoError
}
