// Copyright 2019 The OpenSDS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package azure

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"encoding/hex"
	"github.com/Azure/azure-storage-blob-go/azblob"
	backendpb "github.com/opensds/multi-cloud/backend/proto"
	. "github.com/opensds/multi-cloud/s3/error"
	dscommon "github.com/opensds/multi-cloud/s3/pkg/datastore/common"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	osdss3 "github.com/opensds/multi-cloud/s3/pkg/service"
	"github.com/opensds/multi-cloud/s3/pkg/utils"
	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
	"strconv"
)

// TryTimeout indicates the maximum time allowed for any single try of an HTTP request.
var MaxTimeForSingleHttpRequest = 50 * time.Minute

type AzureAdapter struct {
	backend      *backendpb.BackendDetail
	containerURL azblob.ContainerURL
}

func (ad *AzureAdapter) BucketDelete(ctx context.Context, input *pb.Bucket) error {

	containerURL, _ := ad.createBucketContainerURL(ad.backend.Access, ad.backend.Security, input.Name)

	_, err := containerURL.Delete(ctx, azblob.ContainerAccessConditions{})
	if err != nil {
		log.Error("failed to delete bucket:",err)
		return err
	}
	log.Infof("Successful bucket deletion")
	return nil
}

func (ad *AzureAdapter) BucketCreate(ctx context.Context, input *pb.Bucket) error {

	containerURL, _ := ad.createBucketContainerURL(ad.backend.Access, ad.backend.Security, input.Name)

	// Create the container on the service (with no metadata and no public access)
	_, err := containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	if err != nil {
		log.Error("failed to create bucket:",err)
		return err
	}
	log.Infof("Successful bucket creation")
	return nil

}

//creates containerURL with bucketname
func (ad *AzureAdapter) createBucketContainerURL(accountName string, accountKey string, bucketName string) (azblob.ContainerURL, error) {
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Error("create credential[Azure Blob] failed, err:%v\n", err)
		return azblob.ContainerURL{}, err
	}

	pipeline := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	u, _ := url.Parse("https://" + accountName + ".blob.core.windows.net")
	serviceURL := azblob.NewServiceURL(*u, pipeline)
	containerURL := serviceURL.NewContainerURL(bucketName) // Container names require lowercase
	return containerURL, nil

}

/*func Init(backend *backendpb.BackendDetail) *AzureAdapter {
	endpoint := backend.Endpoint
	AccessKeyID := backend.Access
	AccessKeySecret := backend.Security
	ad := AzureAdapter{}
	containerURL, err := ad.createContainerURL(endpoint, AccessKeyID, AccessKeySecret)
	if err != nil {
		log.Infof("AzureAdapter Init container URL faild:%v\n", err)
		return nil
	}
	adap := &AzureAdapter{backend: backend, containerURL: containerURL}
	log.Log("AzureAdapter Init succeed, container URL:", containerURL.String())
	return adap
}*/

func (ad *AzureAdapter) createContainerURL(endpoint string, acountName string, accountKey string) (azblob.ContainerURL,
	error) {
	credential, err := azblob.NewSharedKeyCredential(acountName, accountKey)

	if err != nil {
		log.Errorf("create credential[Azure Blob] failed, err:%v\n", err)
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

func (ad *AzureAdapter) Put(ctx context.Context, stream io.Reader, object *pb.Object) (dscommon.PutResult, error) {
	objectId := object.ObjectKey
	containerURL, _ := ad.createBucketContainerURL(ad.backend.Access, ad.backend.Security, object.BucketName)
	blobURL := containerURL.NewBlockBlobURL(objectId)
	result := dscommon.PutResult{}
	userMd5 := dscommon.GetMd5FromCtx(ctx)
	log.Infof("put object[Azure Blob], objectId:%s, blobURL:%v, userMd5:%s, size:%d\n", objectId, blobURL, userMd5, object.Size)

	log.Infof("put object[Azure Blob] begin, objectId:%s\n", objectId)
	options := azblob.UploadStreamToBlockBlobOptions{BufferSize: 2 * 1024 * 1024, MaxBuffers: 2}

	uploadResp, err := azblob.UploadStreamToBlockBlob(ctx, stream, blobURL, options)
	log.Infof("put object[Azure Blob] end, objectId:%s\n", objectId)
	if err != nil {
		log.Errorf("put object[Azure Blob], objectId:%s, err:%v\n", objectId, err)
		return result, ErrPutToBackendFailed
	}
	if uploadResp.Response().StatusCode != http.StatusCreated {
		log.Errorf("put object[Azure Blob], objectId:%s, StatusCode:%d\n", objectId, uploadResp.Response().StatusCode)
		return result, ErrPutToBackendFailed
	}

	if object.Tier == 0 {
		// default
		object.Tier = utils.Tier1
	}
	storClass, err := osdss3.GetNameFromTier(object.Tier, utils.OSTYPE_Azure)
	if err != nil {
		log.Infof("translate tier[%d] to aws storage class failed\n", object.Tier)
		return result, ErrInternalError
	}

	resultMd5 := uploadResp.Response().Header.Get("Content-MD5")
	resultMd5Bytes, err := base64.StdEncoding.DecodeString(resultMd5)
	if err != nil {
		log.Errorf("decode Content-MD5 failed, err:%v\n", err)
		return result, ErrBadDigest
	}
	decodedMd5 := hex.EncodeToString(resultMd5Bytes)
	if userMd5 != "" && userMd5 != decodedMd5 {
		log.Error("### MD5 not match, resultMd5:", resultMd5, ", decodedMd5:", decodedMd5, ", userMd5:", userMd5)
		return result, ErrBadDigest
	}

	// Currently, only support Hot
	_, err = blobURL.SetTier(ctx, azblob.AccessTierType(storClass), azblob.LeaseAccessConditions{})
	if err != nil {
		log.Errorf("set azure blob tier[%s] failed:%v\n", object.Tier, err)
		return result, ErrPutToBackendFailed
	}

	result.UpdateTime = time.Now().Unix()
	result.ObjectId = objectId
	result.Etag = decodedMd5
	result.Meta = uploadResp.Version()
	result.Written = object.Size
	log.Infof("upload object[Azure Blob] succeed, objectId:%s, UpdateTime is:%v\n", objectId, result.UpdateTime)

	return result, nil
}

func (ad *AzureAdapter) Get(ctx context.Context, object *pb.Object, start int64, end int64) (io.ReadCloser, error) {
	bucket := object.BucketName
	log.Infof("get object[Azure Blob], bucket:%s, objectId:%s\n", bucket, object.ObjectId)
	containerURL, _ := ad.createBucketContainerURL(ad.backend.Access, ad.backend.Security, object.BucketName)
	blobURL := containerURL.NewBlobURL(object.ObjectId)

	count := end - start + 1
	log.Infof("blobURL:%v, size:%d, start=%d, end=%d, count=%d\n", blobURL, object.Size, start, end, count)
	downloadResp, err := blobURL.Download(ctx, start, count, azblob.BlobAccessConditions{}, false)
	if err != nil {
		log.Errorf("get object[Azure Blob] failed, objectId:%s, err:%v\n", object.ObjectId, err)
		return nil, ErrGetFromBackendFailed
	}

	log.Infof("get object[Azure Blob] successfully, objectId:%s\n", object.ObjectId)
	return downloadResp.Response().Body, nil
}

func (ad *AzureAdapter) Delete(ctx context.Context, input *pb.DeleteObjectInput) error {
	bucket := input.Bucket
	objectId := input.Key
	log.Infof("delete object[Azure Blob], objectId:%s, bucket:%s\n", objectId, bucket)
	containerURL, _ := ad.createBucketContainerURL(ad.backend.Access, ad.backend.Security, input.Bucket)
	blobURL := containerURL.NewBlockBlobURL(objectId)
	log.Infof("blobURL is %v\n", blobURL)
	delRsp, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{})
	if err != nil {
		if serr, ok := err.(azblob.StorageError); ok { // This error is a Service-specific
			log.Infof("delete service code:%s\n", serr.ServiceCode())
			if string(serr.ServiceCode()) == string(azblob.StorageErrorCodeBlobNotFound) {
				return nil
			}
		}

		log.Errorf("delete object[Azure Blob] failed, objectId:%s, err:%v\n", objectId, err)
		return ErrDeleteFromBackendFailed
	}

	if delRsp.StatusCode() != http.StatusOK && delRsp.StatusCode() != http.StatusAccepted {
		log.Errorf("delete object[Azure Blob] failed, objectId:%s, status code:%d\n", objectId, delRsp.StatusCode())
		return ErrDeleteFromBackendFailed
	}

	log.Infof("delete object[Azure Blob] succeed, objectId:%s\n", objectId)
	return nil
}

func (ad *AzureAdapter) Copy(ctx context.Context, stream io.Reader, target *pb.Object) (result dscommon.PutResult, err error) {
	log.Errorf("copy[Azure Blob] is not supported.")
	err = ErrInternalError
	return
}

func (ad *AzureAdapter) ChangeStorageClass(ctx context.Context, object *pb.Object, newClass *string) error {
	objectId := object.ObjectId
	containerURL, _ := ad.createBucketContainerURL(ad.backend.Access, ad.backend.Security, object.BucketName)
	blobURL := containerURL.NewBlobURL(objectId)
	log.Infof("change storage class[Azure Blob], object=[%s], objectId:%s, blobURL is %v\n", object, objectId, blobURL)

	var res *azblob.BlobSetTierResponse
	var err error
	switch *newClass {
	case string(azblob.AccessTierHot):
		res, err = blobURL.SetTier(ctx, azblob.AccessTierHot, azblob.LeaseAccessConditions{})
	case string(azblob.AccessTierCool):
		res, err = blobURL.SetTier(ctx, azblob.AccessTierCool, azblob.LeaseAccessConditions{})
	case string(azblob.AccessTierArchive):
		res, err = blobURL.SetTier(ctx, azblob.AccessTierArchive, azblob.LeaseAccessConditions{})
	default:
		log.Errorf("change storage class[Azure Blob] of object[%s] to %s failed, err: invalid storage class.\n",
			object.ObjectKey, newClass)
		return ErrInvalidStorageClass
	}
	if err != nil {
		log.Errorf("change storage class[Azure Blob] of object[%s] to %s failed, err:%v\n", object.ObjectKey,
			newClass, err)
		return ErrInternalError
	} else {
		log.Errorf("change storage class[Azure Blob] of object[%s] to %s succeed, res:%v\n", object.ObjectKey,
			newClass, res.Response())
	}

	return nil
}

func (ad *AzureAdapter) GetObjectInfo(bucketName string, key string, context context.Context) (*pb.Object, error) {
	object := pb.Object{}
	object.BucketName = bucketName
	object.ObjectKey = key
	return &object, nil
}

func (ad *AzureAdapter) InitMultipartUpload(ctx context.Context, object *pb.Object) (*pb.MultipartUpload, error) {
	bucket := object.BucketName
	log.Infof("bucket is %v\n", bucket)
	multipartUpload := &pb.MultipartUpload{}
	multipartUpload.Key = object.ObjectKey

	multipartUpload.Bucket = object.BucketName
	multipartUpload.UploadId = object.ObjectKey + "_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	multipartUpload.ObjectId = object.BucketName + "/" + object.ObjectKey
	return multipartUpload, nil
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

func (ad *AzureAdapter) UploadPart(ctx context.Context, stream io.Reader, multipartUpload *pb.MultipartUpload,
	partNumber int64, upBytes int64) (*model.UploadPartResult, error) {
	bucket := ad.backend.BucketName
	log.Infof("upload part[Azure Blob], bucket:%s, objectId:%s, partNumber:%d\n", bucket, multipartUpload.ObjectId, partNumber)

	blobURL := ad.containerURL.NewBlockBlobURL(multipartUpload.ObjectId)
	base64ID := ad.Int64ToBase64(partNumber)
	bytess, _ := ioutil.ReadAll(stream)
	log.Debugf("blobURL=%+v\n", blobURL)
	rsp, err := blobURL.StageBlock(ctx, base64ID, bytes.NewReader(bytess), azblob.LeaseAccessConditions{}, nil)
	if err != nil {
		log.Errorf("stage block[#%d,base64ID:%s] failed:%v\n", partNumber, base64ID, err)
		return nil, ErrPutToBackendFailed
	}

	etag := hex.EncodeToString(rsp.ContentMD5())
	log.Infof("stage block[#%d,base64ID:%s] succeed, etag:%s.\n", partNumber, base64ID, etag)
	result := &model.UploadPartResult{PartNumber: partNumber, ETag: etag}

	return result, nil
}

func (ad *AzureAdapter) CompleteMultipartUpload(ctx context.Context, multipartUpload *pb.MultipartUpload,
	completeUpload *model.CompleteMultipartUpload) (*model.CompleteMultipartUploadResult, error) {
	bucket := ad.backend.BucketName
	result := model.CompleteMultipartUploadResult{}
	result.Bucket = multipartUpload.Bucket
	result.Key = multipartUpload.Key
	result.Location = ad.backend.Name
	log.Infof("complete multipart upload[Azure Blob], bucket:%s, objectId:%s\n", bucket, multipartUpload.ObjectId)

	blobURL := ad.containerURL.NewBlockBlobURL(multipartUpload.ObjectId)
	var completeParts []string
	for _, p := range completeUpload.Parts {
		base64ID := ad.Int64ToBase64(p.PartNumber)
		completeParts = append(completeParts, base64ID)
	}
	log.Debugf("commit block list, blobURL:%+v, completeParts:%+v\n", blobURL, completeParts)
	_, err := blobURL.CommitBlockList(ctx, completeParts, azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{})
	if err != nil {
		log.Errorf("commit blocks[bucket:%s, objectId:%s] failed:%v\n", bucket, multipartUpload.ObjectId, err)
		return nil, ErrBackendCompleteMultipartFailed
	} else {
		storClass, err := osdss3.GetNameFromTier(multipartUpload.Tier, utils.OSTYPE_Azure)
		if err != nil {
			log.Errorf("translate tier[%d] to aws storage class failed\n", multipartUpload.Tier)
			return nil, ErrInternalError
		}

		// set tier
		_, err = blobURL.SetTier(ctx, azblob.AccessTierType(storClass), azblob.LeaseAccessConditions{})
		if err != nil {
			log.Errorf("set blob[objectId:%s] tier failed:%v\n", multipartUpload.ObjectId, err)
			return nil, ErrBackendCompleteMultipartFailed
		}
	}

	log.Infof("complete multipart upload[Azure Blob], bucket:%s, objectId:%s succeed\n",
		bucket, multipartUpload.ObjectId)
	return &result, nil
}

func (ad *AzureAdapter) AbortMultipartUpload(ctx context.Context, multipartUpload *pb.MultipartUpload) error {
	bucket := ad.backend.BucketName
	log.Infof("no need to abort multipart upload[objkey:%s].\n", bucket)
	return nil
}

func (ad *AzureAdapter) ListParts(ctx context.Context, multipartUpload *pb.ListParts) (*model.ListPartsOutput, error) {
	return nil, ErrNotImplemented
}

func (ad *AzureAdapter) BackendCheck(ctx context.Context, backendDetail *pb.BackendDetailS3) error {

	object := &pb.Object{
		BucketName: backendDetail.BucketName,
		ObjectKey:  "emptyContainer/",
	}

	bs := []byte{0}
	stream := bytes.NewReader(bs)

	_, err := ad.Put(ctx, stream, object)

	if err != nil {
		log.Debug("failed to put object[Azure Blob]:", err)
		return err
	}

	input := &pb.DeleteObjectInput{
		Bucket: backendDetail.BucketName,
		Key:    "EmptyContainer/",
	}

	err = ad.Delete(ctx, input)
	if err != nil {
		log.Debug("failed to delete object[Azure Blob],\n", err)
		return err
	}
	log.Debug("create and delete object is successful\n")
	return nil
}

func (ad *AzureAdapter) Restore(ctx context.Context, inp *pb.Restore) error {
	return ErrNotImplemented
}

func (ad *AzureAdapter) Close() error {
	// TODO:
	return nil
}
