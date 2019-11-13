package storage

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"sort"
	"strconv"

	s3err "github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/common"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/meta/types"
	"github.com/opensds/multi-cloud/s3/pkg/model"
	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

const (
	MAX_PART_SIZE   = 5 << 30 // 5GB
	MIN_PART_SIZE   = 5 << 20 // 5MB
	MIN_PART_NUMBER = 1
	MAX_PART_NUMBER = 10000
)

/*
* Below is the process of multipart upload:
* 1. InitMultipartUpload will return a upload id and object id, and the caller should save them.
* 2. caller should call UploadPart and input upload id and part number and then transfer the data.
* backend will save the upload id and the part number and other related information.
* 3. caller will call CompleteMultipartUpload and input the upload id, the backend
* will mark all the parts identified by upload id as completed. If the caller calls
* AbortMultipartUpload, backend will remove all the parts belongs to the upload id.
*
 */

func (yig *YigStorage) InitMultipartUpload(ctx context.Context, object *pb.Object) (*pb.MultipartUpload, error) {
	uploadId := uploadId2Str(yig.idGen.GetId())
	mu := &pb.MultipartUpload{
		Bucket:   object.BucketName,
		Key:      object.ObjectKey,
		UploadId: uploadId,
		ObjectId: uploadId,
	}
	return mu, nil
}

/*
* ctx: the context should contain below information: md5
*
 */

func (yig *YigStorage) UploadPart(ctx context.Context, stream io.Reader, multipartUpload *pb.MultipartUpload,
	partNumber int, upBytes int64) (*model.UploadPartResult, error) {
	// check the limiation https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/qfacts.html
	if upBytes > MAX_PART_SIZE {
		log.Errorf("UploadPart(%s, %s, %s, %d) failed, the size %d exceeds the maximum size.", multipartUpload.Bucket, multipartUpload.Key, multipartUpload.UploadId, partNumber, upBytes)
		return nil, s3err.ErrEntityTooLarge
	}
	if partNumber < MIN_PART_NUMBER || partNumber > MAX_PART_NUMBER {
		log.Errorf("UploadPart(%s, %s, %s, %d) failed, got invalid partNumber.", multipartUpload.Bucket, multipartUpload.Key, multipartUpload.UploadId, partNumber)
		return nil, s3err.ErrInvalidPart
	}
	uploadId, err := str2UploadId(multipartUpload.UploadId)
	if err != nil {
		log.Errorf("UploadPart(%s, %s, %s, %d) failed, failed to convert uploadId to int64, err: %v", multipartUpload.Bucket, multipartUpload.Key, multipartUpload.UploadId, partNumber, err)
		return nil, s3err.ErrNoSuchUpload
	}
	// TODO we need check whether the part number exists or not, if it already exists, we need to override it.
	md5Writer := md5.New()
	inputMd5 := ""
	if val := ctx.Value(common.CONTEXT_KEY_MD5); val != nil {
		inputMd5 = val.(string)
	}

	limitedDataReader := io.LimitReader(stream, upBytes)
	cephCluster, poolName := yig.PickOneClusterAndPool(multipartUpload.Bucket, multipartUpload.Key, upBytes, false)
	if cephCluster == nil {
		log.Errorf("UploadPart(%s, %s, %s, %d) failed, cannot find the cluster", multipartUpload.Bucket, multipartUpload.Key, multipartUpload.UploadId, partNumber)
		return nil, s3err.ErrInternalError
	}
	oid := cephCluster.GetUniqUploadName()
	dataReader := io.TeeReader(limitedDataReader, md5Writer)
	bytesWritten, err := cephCluster.Put(poolName, oid, dataReader)
	if err != nil {
		log.Errorf("failed to UploadPart(%s, %s, %s, %d), err: %v", multipartUpload.Bucket, multipartUpload.Key, multipartUpload.UploadId, partNumber, err)
		return nil, err
	}
	// Should metadata update failed, add `maybeObjectToRecycle` to `RecycleQueue`,
	// so the object in Ceph could be removed asynchronously
	maybeObjectToRecycle := objectToRecycle{
		location: cephCluster.Name,
		pool:     poolName,
		objectId: oid,
	}
	if bytesWritten < upBytes {
		RecycleQueue <- maybeObjectToRecycle
		log.Errorf("failed to UploadPart(%s, %s, %s, %d), written: %d, total: %d", multipartUpload.Bucket, multipartUpload.Key, multipartUpload.UploadId, partNumber, bytesWritten, upBytes)
		return nil, s3err.ErrIncompleteBody
	}
	calculatedMd5 := hex.EncodeToString(md5Writer.Sum(nil))
	if inputMd5 != "" && inputMd5 != calculatedMd5 {
		RecycleQueue <- maybeObjectToRecycle
		log.Errorf("failed to UploadPart(%s, %s, %s, %d), input md5: %s, calculated: %s", multipartUpload.Bucket, multipartUpload.Key, multipartUpload.UploadId, partNumber, inputMd5, calculatedMd5)
		return nil, s3err.ErrBadDigest
	}
	partInfo := &types.PartInfo{
		UploadId: uploadId,
		PartNum:  uint(partNumber),
		ObjectId: oid,
		Location: cephCluster.Name,
		Pool:     poolName,
		Size:     uint64(upBytes),
		Etag:     calculatedMd5,
		Flag:     types.MULTIPART_UPLOAD_IN_PROCESS,
	}
	err = yig.MetaStorage.PutPart(partInfo)
	if err != nil {
		log.Errorf("failed to  put part for %s, %s, %s, %d, err: %v", multipartUpload.Bucket, multipartUpload.Key, multipartUpload.UploadId, partNumber, err)
		return nil, err
	}
	result := &model.UploadPartResult{
		PartNumber: partNumber,
		ETag:       calculatedMd5,
	}
	return result, nil
}

func (yig *YigStorage) CompleteMultipartUpload(ctx context.Context, multipartUpload *pb.MultipartUpload,
	completeUpload *model.CompleteMultipartUpload) (*model.CompleteMultipartUploadResult, error) {
	uploadId, err := str2UploadId(multipartUpload.UploadId)
	if err != nil {
		log.Errorf("failed to convert uploadId(%s) to int64, err: %v", multipartUpload.UploadId, err)
		return nil, err
	}
	parts, err := yig.MetaStorage.ListParts(uploadId)
	if err != nil {
		log.Errorf("failed to list parts for uploadId(%d), err: %v", uploadId, err)
		return nil, err
	}

	// check whether the given parts are already recorded in db.
	if len(parts) != len(completeUpload.Parts) {
		log.Errorf("input len(parts): %d, while we recorded len(parts): %d", len(completeUpload.Parts), len(parts))
		return nil, s3err.ErrInvalidPartOrder
	}

	md5Writer := md5.New()
	totalSize := uint64(0)
	for k, part := range completeUpload.Parts {
		// chech whether the part number starts at 1 and increases one by one.
		if part.PartNumber != k+1 {
			log.Errorf("got invalid part[%d, %s] for uploadId %d with idx %d", part.PartNumber, part.ETag, uploadId, k)
			return nil, s3err.ErrInvalidPart
		}
		// check whether the given parts are already recorded in db.
		i := sort.Search(len(parts), func(i int) bool { return parts[i].PartNum >= uint(part.PartNumber) })
		if i >= len(parts) || parts[i].PartNum != uint(part.PartNumber) {
			log.Errorf("we cannot find the part[%d, %s] for uploadId %d", part.PartNumber, part.ETag, uploadId)
			return nil, s3err.ErrInvalidPart
		}
		// check whether etag is matched.
		if part.ETag != parts[i].Etag {
			log.Errorf("got invalid part(%d, %s), the etag recorded is %s", part.PartNumber, part.ETag, parts[i].Etag)
			return nil, s3err.ErrInvalidPart
		}
		// caclulate the md5 of etag for the whole object.
		var etagBytes []byte
		etagBytes, err = hex.DecodeString(part.ETag)
		if err != nil {
			log.Errorf("failed to decode etag of part(%d, %s) of uploadId(%d)", part.PartNumber, part.ETag, uploadId)
			return nil, s3err.ErrInvalidPart
		}
		md5Writer.Write(etagBytes)
		// check whether the size of each part except the last one is >= 5M and <= 5G.
		if parts[i].Size < MIN_PART_SIZE && i < len(parts)-1 {
			log.Errorf("got invalid size %d for part(%d, %s) of uploadId %d", parts[i].Size, parts[i].PartNum, parts[i].Etag, uploadId)
			return nil, s3err.ErrInvalidPart
		}
		// complete an already completed uploadId, 404 will be returned.
		if parts[i].Flag == types.MULTIPART_UPLOAD_COMPLETE {
			log.Errorf("got completed part(%d) for uploadId %d", parts[i].PartNum, uploadId)
			return nil, s3err.ErrNoSuchUpload
		}

		parts[i].Flag = types.MULTIPART_UPLOAD_COMPLETE
		parts[i].Offset = totalSize
		totalSize += parts[i].Size
	}
	// completes the parts.
	err = yig.MetaStorage.CompleteParts(uploadId, parts)
	if err != nil {
		log.Errorf("failed to complete parts for uploadId(%d), err: %v", uploadId, err)
		return nil, err
	}
	result := &model.CompleteMultipartUploadResult{
		Bucket: multipartUpload.Bucket,
		Key:    multipartUpload.Key,
	}
	result.ETag = hex.EncodeToString(md5Writer.Sum(nil))
	result.ETag += "-" + strconv.Itoa(len(parts))
	return result, nil
}

func (yig *YigStorage) AbortMultipartUpload(ctx context.Context, multipartUpload *pb.MultipartUpload) error {
	uploadId, err := str2UploadId(multipartUpload.UploadId)
	if err != nil {
		log.Errorf("failed to AbortMultipartUpload for %s, it was fail to parse uploadId, err: %v", multipartUpload.UploadId, err)
		return err
	}
	parts, err := yig.MetaStorage.ListParts(uploadId)
	if err != nil {
		log.Errorf("failed to get parts for uploadId(%d), err: %v", uploadId, err)
		return err
	}

	// remove the parts info from meta.
	err = yig.MetaStorage.DeleteParts(uploadId)
	if err != nil {
		log.Errorf("failed to delete parts from meta for uploadId(%d), err: %v", uploadId, err)
		return err
	}

	// remove all the parts from ceph cluster.
	for _, p := range parts {
		RecycleQueue <- objectToRecycle{
			location: p.Location,
			pool:     p.Pool,
			objectId: p.ObjectId,
		}
	}

	return nil
}

func uploadId2Str(id int64) string {
	return strconv.FormatUint(uint64(id), 16)
}

func str2UploadId(id string) (uint64, error) {
	return strconv.ParseUint(id, 16, 64)
}
