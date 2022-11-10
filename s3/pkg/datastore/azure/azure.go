// Copyright 2019 The soda Authors.
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
	"strconv"

	"github.com/Azure/azure-storage-blob-go/azblob"
	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"

	backendpb "github.com/soda/multi-cloud/backend/proto"
	. "github.com/soda/multi-cloud/s3/error"
	dscommon "github.com/soda/multi-cloud/s3/pkg/datastore/common"
	"github.com/soda/multi-cloud/s3/pkg/model"
	osdss3 "github.com/soda/multi-cloud/s3/pkg/service"
	"github.com/soda/multi-cloud/s3/pkg/utils"
	pb "github.com/soda/multi-cloud/s3/proto"
)

// TryTimeout indicates the maximum time allowed for any single try of an HTTP request.
var MaxTimeForSingleHttpRequest = 50 * time.Minute

const sampleBucket string = "sample"

type AzureAdapter struct {
	backend      *backendpb.BackendDetail
	containerURL azblob.ContainerURL
}

func (ad *AzureAdapter) BucketDelete(ctx context.Context, input *pb.Bucket) error {

	containerURL, _ := ad.createBucketContainerURL(ad.backend.Access, ad.backend.Security, input.Name)

	_, err := containerURL.Delete(ctx, azblob.ContainerAccessConditions{})
	if err != nil {
		log.Error("failed to delete bucket:", err)
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
		log.Error("failed to create bucket:", err)
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
	storageClass := dscommon.GetStorClassFromCtx(ctx)
	log.Infof("Put object[Azure Blob], objectId:%s, blobURL:%v, userMd5:%s, size:%d storageClass=%s", objectId, blobURL, userMd5, object.Size, storageClass)

	log.Infof("Put object[Azure Blob] begin, objectId:%s\n", objectId)
	options := azblob.UploadStreamToBlockBlobOptions{BufferSize: 2 * 1024 * 1024, MaxBuffers: 2}

	uploadResp, err := azblob.UploadStreamToBlockBlob(ctx, stream, blobURL, options)
	log.Infof("Put object[Azure Blob] end, objectId:%s\n", objectId)
	if err != nil {
		log.Errorf("put object[Azure Blob], objectId:%s, err:%v\n", objectId, err)
		return result, ErrPutToBackendFailed
	}
	if uploadResp.Response().StatusCode != http.StatusCreated {
		log.Errorf("put object[Azure Blob], objectId:%s, StatusCode:%d\n", objectId, uploadResp.Response().StatusCode)
		return result, ErrPutToBackendFailed
	}

	var storClass string

	if storageClass != "" {
		storClass = storageClass
	} else {
		if object.Tier == utils.NO_TIER {
			// default
			log.Debugf("No tier is set. Setting it to default:[%d]", utils.NO_TIER)
			object.Tier = utils.Tier1
		}
		storClass, err = osdss3.GetNameFromTier(object.Tier, utils.OSTYPE_Azure)
		if err != nil {
			log.Errorf("translate tier[%d] to azure storage class failed", object.Tier)
			return result, ErrInternalError
		}
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
	log.Infof("Get object[Azure Blob], bucket:%s, objectId:%s\n", bucket, object.ObjectId)
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
	log.Infof("Delete object[Azure Blob], objectId:%s, bucket:%s\n", objectId, bucket)
	containerURL, _ := ad.createBucketContainerURL(ad.backend.Access, ad.backend.Security, input.Bucket)
	blobURL := containerURL.NewBlockBlobURL(objectId)
	log.Infof("blobURL is %v\n", blobURL)
	delRsp, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{})
	if err != nil {
		if serr, ok := err.(azblob.StorageError); ok { // This error is a Service-specific
			log.Infof("Delete service code:%s\n", serr.ServiceCode())
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
	containerURL, urlerr := ad.createBucketContainerURL(ad.backend.Access, ad.backend.Security, object.BucketName)
	if urlerr != nil {
		log.Errorf("failed to create bucket container url for azure:%v", urlerr)
		return urlerr
	}
	blobURL := containerURL.NewBlockBlobURL(objectId)
	log.Infof("Change storage class[Azure Blob], objectId:%s, storageClass:%s blobURL is %v", objectId, *newClass, blobURL)

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
		log.Errorf("change storage class[Azure Blob] of object[%s] to %s failed, err: invalid storage class.",
			object.ObjectKey, newClass)
		return ErrInvalidStorageClass
	}
	if err != nil {
		log.Errorf("change storage class[Azure Blob] of object[%s] to %s failed, err:%v", object.ObjectKey,
			newClass, err)
		return ErrInternalError
	} else {
		log.Infof("Change storage class[Azure Blob] of object[%s] to %s succeed, res:%v", object.ObjectKey,
			*newClass, res.Response())
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
	log.Infof("Bucket is %v", bucket)
	multipartUpload := &pb.MultipartUpload{}
	multipartUpload.Key = object.ObjectKey

	multipartUpload.Bucket = object.BucketName
	multipartUpload.UploadId = object.ObjectKey + "_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	multipartUpload.ObjectId = object.ObjectKey
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
	bucket := multipartUpload.Bucket
	log.Infof("Upload part[Azure Blob], bucket:%s, objectId:%s, partNumber:%d", bucket, multipartUpload.ObjectId, partNumber)

	containerURL, err1 := ad.createBucketContainerURL(ad.backend.Access, ad.backend.Security, multipartUpload.Bucket)
	if err1 != nil {
		log.Errorf("error in containerURL creation:", err1)
		return nil, err1
	}

	blobURL := containerURL.NewBlockBlobURL(multipartUpload.ObjectId)
	base64ID := ad.Int64ToBase64(partNumber)
	bytess, _ := ioutil.ReadAll(stream)
	log.Debugf("BlobURL=%+v", blobURL)
	rsp, err := blobURL.StageBlock(ctx, base64ID, bytes.NewReader(bytess), azblob.LeaseAccessConditions{}, nil)
	if err != nil {
		log.Errorf("stage block[#%d,base64ID:%s] failed:%v", partNumber, base64ID, err)
		return nil, ErrPutToBackendFailed
	}

	etag := hex.EncodeToString(rsp.ContentMD5())
	log.Infof("Stage block[#%d,base64ID:%s] succeed, etag:%s.", partNumber, base64ID, etag)
	result := &model.UploadPartResult{PartNumber: partNumber, ETag: etag}

	return result, nil
}

func (ad *AzureAdapter) CompleteMultipartUpload(ctx context.Context, multipartUpload *pb.MultipartUpload,
	completeUpload *model.CompleteMultipartUpload) (*model.CompleteMultipartUploadResult, error) {
	bucket := multipartUpload.Bucket
	result := model.CompleteMultipartUploadResult{}
	result.Bucket = multipartUpload.Bucket
	result.Key = multipartUpload.Key
	result.Location = ad.backend.Name
	log.Infof("Complete multipart upload[Azure Blob], bucket:%s, objectId:%s", bucket, multipartUpload.ObjectId)

	containerURL, err1 := ad.createBucketContainerURL(ad.backend.Access, ad.backend.Security, multipartUpload.Bucket)
	if err1 != nil {
		log.Errorf("error in containerURL creation:", err1)
		return nil, err1
	}
	blobURL := containerURL.NewBlockBlobURL(multipartUpload.ObjectId)
	var completeParts []string
	for _, p := range completeUpload.Parts {
		base64ID := ad.Int64ToBase64(p.PartNumber)
		completeParts = append(completeParts, base64ID)
	}
	log.Debugf("Commit block list, blobURL:%+v, completeParts:%+v", blobURL, completeParts)
	_, err := blobURL.CommitBlockList(ctx, completeParts, azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{})
	if err != nil {
		log.Errorf("commit blocks[bucket:%s, objectId:%s] failed:%v", bucket, multipartUpload.ObjectId, err)
		return nil, ErrBackendCompleteMultipartFailed
	} else {
		storClass, err := osdss3.GetNameFromTier(multipartUpload.Tier, utils.OSTYPE_Azure)
		if err != nil {
			log.Errorf("translate tier[%d] to aws storage class failed", multipartUpload.Tier)
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
	bucket := multipartUpload.Bucket
	log.Infof("no need to abort multipart upload[objkey:%s].\n", bucket)
	return nil
}

func (ad *AzureAdapter) ListParts(ctx context.Context, multipartUpload *pb.ListParts) (*model.ListPartsOutput, error) {
	return nil, ErrNotImplemented
}

func (ad *AzureAdapter) BackendCheck(ctx context.Context, backendDetail *pb.BackendDetailS3) error {
	randId := uuid.NewV4().String()
	input := &pb.Bucket{
		Name: sampleBucket + randId,
	}

	err := ad.BucketCreate(ctx, input)
	if err != nil {
		log.Error("failed to create sample bucket :", err)
		return err
	}

	log.Debug("Create sample bucket is successul")
	err = ad.BucketDelete(ctx, input)
	if err != nil {
		log.Error("failed to delete sample bucket :", err)
		return err
	}

	log.Debug("Delete sample bucket is successful")
	return nil
}

func (ad *AzureAdapter) Restore(ctx context.Context, inp *pb.Restore) error {
	className := inp.StorageClass
	log.Infof("Restore the object to storage class [%s]", className)
	objId := inp.ObjectKey
	obj := &pb.Object{
		ObjectId:   objId,
		ObjectKey:  inp.ObjectKey,
		BucketName: inp.BucketName,
	}
	err := ad.ChangeStorageClass(ctx, obj, &className)
	if err != nil {
		log.Error("error [%v] in changing the storage class of the object [%s]", err, inp.ObjectKey)
		return err
	}
	return nil
}

func (ad *AzureAdapter) Close() error {
	// TODO:
	return nil
}
