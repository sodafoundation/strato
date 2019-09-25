package storage

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"sync"
	"time"

	. "github.com/opensds/multi-cloud/s3/error"
	dscommon "github.com/opensds/multi-cloud/s3/pkg/datastore/common"
	"github.com/opensds/multi-cloud/s3/pkg/helper"
	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

var latestQueryTime [2]time.Time // 0 is for SMALL_FILE_POOLNAME, 1 is for BIG_FILE_POOLNAME
const CLUSTER_MAX_USED_SPACE_PERCENT = 85

func (yig *YigStorage) PickOneClusterAndPool(bucket string, object string, size int64, isAppend bool) (cluster *CephStorage,
	poolName string) {

	var idx int
	if isAppend {
		poolName = BIG_FILE_POOLNAME
		idx = 1
	} else if size < 0 { // request.ContentLength is -1 if length is unknown
		poolName = BIG_FILE_POOLNAME
		idx = 1
	} else if size < BIG_FILE_THRESHOLD {
		poolName = SMALL_FILE_POOLNAME
		idx = 0
	} else {
		poolName = BIG_FILE_POOLNAME
		idx = 1
	}
	var needCheck bool
	queryTime := latestQueryTime[idx]
	if time.Since(queryTime).Hours() > 24 { // check used space every 24 hours
		latestQueryTime[idx] = time.Now()
		needCheck = true
	}
	var totalWeight int
	clusterWeights := make(map[string]int, len(yig.DataStorage))
	for fsid, _ := range yig.DataStorage {
		cluster, err := yig.MetaStorage.GetCluster(fsid, poolName)
		if err != nil {
			log.Debug("Error getting cluster: ", err)
			continue
		}
		if cluster.Weight == 0 {
			continue
		}
		if needCheck {
			pct, err := yig.DataStorage[fsid].GetUsedSpacePercent()
			if err != nil {
				log.Error("Error getting used space: ", err, "fsid: ", fsid)
				continue
			}
			if pct > CLUSTER_MAX_USED_SPACE_PERCENT {
				log.Error("Cluster used space exceed ", CLUSTER_MAX_USED_SPACE_PERCENT, fsid)
				continue
			}
		}
		totalWeight += cluster.Weight
		clusterWeights[fsid] = cluster.Weight
	}
	if len(clusterWeights) == 0 || totalWeight == 0 {
		log.Warn("Error picking cluster from table cluster in DB! Use first cluster in config to write.")
		for _, c := range yig.DataStorage {
			cluster = c
			break
		}
		return
	}
	N := rand.Intn(totalWeight)
	n := 0
	for fsid, weight := range clusterWeights {
		n += weight
		if n > N {
			cluster = yig.DataStorage[fsid]
			break
		}
	}
	return
}

func (yig *YigStorage) GetClusterByFsName(fsName string) (cluster *CephStorage, err error) {
	if c, ok := yig.DataStorage[fsName]; ok {
		cluster = c
	} else {
		err = errors.New("Cannot find specified ceph cluster: " + fsName)
	}
	return
}

/*this pool is for download only */
var (
	downloadBufPool sync.Pool
)

func init() {
	downloadBufPool.New = func() interface{} {
		return make([]byte, helper.CONFIG.DownLoadBufPoolSize)
	}
}

// temp comment
/*func generateTransWholeObjectFunc(cephCluster *CephStorage, object *types.Object) func(io.Writer) error {
	getWholeObject := func(w io.Writer) error {
		reader, err := cephCluster.getReader(object.Pool, object.ObjectId, 0, object.Size)
		if err != nil {
			return nil
		}
		defer reader.Close()

		buf := downloadBufPool.Get().([]byte)
		_, err = io.CopyBuffer(w, reader, buf)
		downloadBufPool.Put(buf)
		return err
	}
	return getWholeObject
}*/

// temp comment.
/*func generateTransPartObjectFunc(cephCluster *CephStorage, object *types.Object, part *types.Part, offset, length int64) func(io.Writer) error {
	getNormalObject := func(w io.Writer) error {
		var oid string
		if part != nil {
			oid = part.ObjectId
		} else {
			oid = object.ObjectId
		}
		reader, err := cephCluster.getReader(object.Pool, oid, offset, length)
		if err != nil {
			return nil
		}
		defer reader.Close()
		buf := downloadBufPool.Get().([]byte)
		_, err = io.CopyBuffer(w, reader, buf)
		downloadBufPool.Put(buf)
		return err
	}
	return getNormalObject
}*/

// temp comment
/*func (yig *YigStorage) GetObject(object *types.Object, startOffset int64,
	length int64, writer io.Writer, sseRequest datatype.SseRequest) (err error) {
	var encryptionKey []byte
	if object.SseType == crypto.S3.String() {
		if yig.KMS == nil {
			return ErrKMSNotConfigured
		}
		key, err := yig.KMS.UnsealKey(yig.KMS.GetKeyID(), object.EncryptionKey,
			crypto.Context{object.BucketName: path.Join(object.BucketName, object.Name)})
		if err != nil {
			return err
		}
		encryptionKey = key[:]
	} else { // SSE-C
		if len(sseRequest.CopySourceSseCustomerKey) != 0 {
			encryptionKey = sseRequest.CopySourceSseCustomerKey
		} else {
			encryptionKey = sseRequest.SseCustomerKey
		}
	}

	if len(object.Parts) == 0 { // this object has only one part
		cephCluster, ok := yig.DataStorage[object.Location]
		if !ok {
			return errors.New("Cannot find specified ceph cluster: " + object.Location)
		}

		transWholeObjectWriter := generateTransWholeObjectFunc(cephCluster, object)

		if object.SseType == "" { // unencrypted object
			transPartObjectWriter := generateTransPartObjectFunc(cephCluster, object, nil, startOffset, length)

			return yig.DataCache.WriteFromCache(object, startOffset, length, writer,
				transPartObjectWriter, transWholeObjectWriter)
		}

		// encrypted object
		normalAligenedGet := func() (io.ReadCloser, error) {
			return cephCluster.getAlignedReader(object.Pool, object.ObjectId,
				startOffset, length)
		}
		reader, err := yig.DataCache.GetAlignedReader(object, startOffset, length, normalAligenedGet,
			transWholeObjectWriter)
		if err != nil {
			return err
		}
		defer reader.Close()

		decryptedReader, err := wrapAlignedEncryptionReader(reader, startOffset, encryptionKey,
			object.InitializationVector)
		if err != nil {
			return err
		}
		//buffer := make([]byte, MAX_CHUNK_SIZE)
		buffer := downloadBufPool.Get().([]byte)
		defer func() {
			bufLen := len(buffer)
			if bufLen > 0 {
				buffer[0] = 0
				for bp := 1; bp < bufLen; bp *= 2 {
					copy(buffer[bp:], buffer[:bp])
				}
			}
			downloadBufPool.Put(buffer)
		}()
		_, err = io.CopyBuffer(writer, decryptedReader, buffer)
		return err
	}

	// multipart uploaded object
	var low int = object.PartsIndex.SearchLowerBound(startOffset)
	if low == -1 {
		low = 1
	} else {
		//parts number starts from 1, so plus 1 here
		low += 1
	}

	for i := low; i <= len(object.Parts); i++ {
		p := object.Parts[i]
		//for high
		if p.Offset > startOffset+length {
			return
		}
		//for low
		{
			var readOffset, readLength int64
			if startOffset <= p.Offset {
				readOffset = 0
			} else {
				readOffset = startOffset - p.Offset
			}
			if p.Offset+p.Size <= startOffset+length {
				readLength = p.Offset + p.Size - readOffset
			} else {
				readLength = startOffset + length - (p.Offset + readOffset)
			}
			cephCluster, ok := yig.DataStorage[object.Location]
			if !ok {
				return errors.New("Cannot find specified ceph cluster: " +
					object.Location)
			}
			if object.SseType == "" { // unencrypted object

				transPartFunc := generateTransPartObjectFunc(cephCluster, object, p, readOffset, readLength)
				err := transPartFunc(writer)
				if err != nil {
					return nil
				}
				continue
			}

			// encrypted object
			err = copyEncryptedPart(object.Pool, p, cephCluster, readOffset, readLength, encryptionKey, writer)
			if err != nil {
				log.Debug("Multipart uploaded object write error:", err)
			}
		}
	}
	return
}*/

// temp comment
/*
func copyEncryptedPart(pool string, part *types.Part, cephCluster *CephStorage, readOffset int64, length int64,
	encryptionKey []byte, targetWriter io.Writer) (err error) {

	reader, err := cephCluster.getAlignedReader(pool, part.ObjectId,
		readOffset, length)
	if err != nil {
		return err
	}
	defer reader.Close()

	decryptedReader, err := wrapAlignedEncryptionReader(reader, readOffset,
		encryptionKey, part.InitializationVector)
	if err != nil {
		return err
	}
	buffer := downloadBufPool.Get().([]byte)
	_, err = io.CopyBuffer(targetWriter, decryptedReader, buffer)
	downloadBufPool.Put(buffer)
	return err
}*/

// Write path:
//                                           +-----------+
// PUT object/part                           |           |   Ceph
//         +---------+------------+----------+ Encryptor +----->
//                   |            |          |           |
//                   |            |          +-----------+
//                   v            v
//                  SHA256      MD5(ETag)
//
// SHA256 is calculated only for v4 signed authentication
// Encryptor is enabled when user set SSE headers

/* ctx should contain below elements:
 * size: object size.
 * encryptionKey:
 * md5: the md5 put by user for the uploading object.
 */
func (yig *YigStorage) Put(ctx context.Context, stream io.Reader, obj *pb.Object) (result dscommon.PutResult,
	err error) {
	// get size from context.
	val := ctx.Value(dscommon.CONTEXT_KEY_SIZE)
	if val == nil {
		return result, ErrIncompleteBody
	}
	size := val.(int64)
	// encryptionKey
	var encryptionKey []byte
	if val = ctx.Value(dscommon.CONTEXT_KEY_ENCRYPTION_KEY); val != nil {
		encryptionKey = val.([]byte)
	}
	// md5 provided by user for uploading object.
	userMd5 := ""
	if val = ctx.Value(dscommon.CONTEXT_KEY_MD5); val != nil {
		userMd5 = val.(string)
	}

	md5Writer := md5.New()

	// Limit the reader to its provided size if specified.
	var limitedDataReader io.Reader
	if size > 0 { // request.ContentLength is -1 if length is unknown
		limitedDataReader = io.LimitReader(stream, size)
	} else {
		limitedDataReader = stream
	}

	cephCluster, poolName := yig.PickOneClusterAndPool(obj.BucketName, obj.ObjectKey, size, false)
	if cephCluster == nil {
		log.Errorf("failed to pick cluster and pool for(%s, %s), err: %v", obj.BucketName, obj.ObjectKey, err)
		return result, ErrInternalError
	}

	objMeta := ObjectMetaInfo{
		Cluster: cephCluster.Name,
		Pool:    poolName,
	}

	metaBytes, err := json.Marshal(objMeta)
	if err != nil {
		log.Errorf("failed to marshal %v for (%s, %s), err: %v", objMeta, obj.BucketName, obj.ObjectKey, err)
		return result, ErrInternalError
	}

	// Mapping a shorter name for the object
	oid := cephCluster.GetUniqUploadName()
	dataReader := io.TeeReader(limitedDataReader, md5Writer)

	var initializationVector []byte
	if len(encryptionKey) != 0 {
		initializationVector, err = newInitializationVector()
		if err != nil {
			return
		}
	}
	storageReader, err := wrapEncryptionReader(dataReader, encryptionKey, initializationVector)
	if err != nil {
		return
	}
	bytesWritten, err := cephCluster.Put(poolName, oid, storageReader)
	if err != nil {
		log.Errorf("failed to put(%s, %s), err: %v", poolName, oid, err)
		return
	}
	// Should metadata update failed, add `maybeObjectToRecycle` to `RecycleQueue`,
	// so the object in Ceph could be removed asynchronously
	maybeObjectToRecycle := objectToRecycle{
		location: cephCluster.Name,
		pool:     poolName,
		objectId: oid,
	}
	if bytesWritten < size {
		RecycleQueue <- maybeObjectToRecycle
		log.Errorf("failed to write objects, already written(%d), total size(%d)", bytesWritten, size)
		return result, ErrIncompleteBody
	}

	calculatedMd5 := hex.EncodeToString(md5Writer.Sum(nil))
	log.Info("### calculatedMd5:", calculatedMd5, "userMd5:", userMd5)
	if userMd5 != "" && userMd5 != calculatedMd5 {
		RecycleQueue <- maybeObjectToRecycle
		return result, ErrBadDigest
	}

	if err != nil {
		RecycleQueue <- maybeObjectToRecycle
		return
	}

	// set the bytes written.
	result.Written = bytesWritten
	result.ObjectId = oid
	result.Etag = calculatedMd5
	result.UpdateTime = time.Now().Unix()
	result.Meta = string(metaBytes)

	return result, nil
}

func (yig *YigStorage) Get(ctx context.Context, object *pb.Object, start int64, end int64) (io.ReadCloser, error) {
	return nil, errors.New("not implemented.")
}

func (yig *YigStorage) Delete(ctx context.Context, object *pb.DeleteObjectInput) error {
	return errors.New("not implemented.")
}

// temp comment
//TODO: Append Support Encryption
/*func (yig *YigStorage) AppendObject(bucketName string, objectName string, credential common.Credential,
	offset uint64, size int64, data io.Reader, metadata map[string]string, acl datatype.Acl,
	sseRequest datatype.SseRequest, storageClass types.StorageClass, objInfo *types.Object) (result datatype.AppendObjectResult, err error) {

	encryptionKey, cipherKey, err := yig.encryptionKeyFromSseRequest(sseRequest, bucketName, objectName)
	log.Info("get encryptionKey:", encryptionKey, "cipherKey:", cipherKey, "err:", err)
	if err != nil {
		return
	}

	//TODO: Append Support Encryption
	encryptionKey = nil

	md5Writer := md5.New()

	// Limit the reader to its provided size if specified.
	var limitedDataReader io.Reader
	if size > 0 { // request.ContentLength is -1 if length is unknown
		limitedDataReader = io.LimitReader(data, size)
	} else {
		limitedDataReader = data
	}

	var cephCluster *CephStorage
	var poolName, oid string
	var initializationVector []byte
	var objSize int64
	if isObjectExist(objInfo) {
		cephCluster, err = yig.GetClusterByFsName(objInfo.Location)
		if err != nil {
			return
		}
		// Every appendable file must be treated as a big file
		poolName = BIG_FILE_POOLNAME
		oid = objInfo.ObjectId
		initializationVector = objInfo.InitializationVector
		objSize = objInfo.Size
		storageClass = objInfo.StorageClass
		helper.Logger.Println(20, "request append oid:", oid, "iv:", initializationVector, "size:", objSize)
	} else {
		// New appendable object
		cephCluster, poolName = yig.PickOneClusterAndPool(bucketName, objectName, size, true)
		if cephCluster == nil || poolName != BIG_FILE_POOLNAME {
			log.Debug("PickOneClusterAndPool error")
			return result, ErrInternalError
		}
		// Mapping a shorter name for the object
		oid = cephCluster.GetUniqUploadName()
		if len(encryptionKey) != 0 {
			initializationVector, err = newInitializationVector()
			if err != nil {
				return
			}
		}
		helper.Logger.Println(20, "request first append oid:", oid, "iv:", initializationVector, "size:", objSize)
	}

	dataReader := io.TeeReader(limitedDataReader, md5Writer)

	storageReader, err := wrapEncryptionReader(dataReader, encryptionKey, initializationVector)
	if err != nil {
		return
	}
	bytesWritten, err := cephCluster.Append(poolName, oid, storageReader, offset, isObjectExist(objInfo))
	if err != nil {
		log.Debug("cephCluster.Append err:", err, poolName, oid, offset)
		return
	}

	if bytesWritten < size {
		return result, ErrIncompleteBody
	}

	calculatedMd5 := hex.EncodeToString(md5Writer.Sum(nil))
	if userMd5, ok := metadata["md5Sum"]; ok {
		if userMd5 != "" && userMd5 != calculatedMd5 {
			return result, ErrBadDigest
		}
	}

	result.Md5 = calculatedMd5

	// TODO validate bucket policy and fancy ACL
	object := &types.Object{
		Name:                 objectName,
		BucketName:           bucketName,
		Location:             cephCluster.Name,
		Pool:                 poolName,
		OwnerId:              credential.UserId,
		Size:                 objSize + bytesWritten,
		ObjectId:             oid,
		LastModifiedTime:     time.Now().UTC(),
		Etag:                 calculatedMd5,
		ContentType:          metadata["Content-Type"],
		ACL:                  acl,
		NullVersion:          true,
		DeleteMarker:         false,
		SseType:              sseRequest.Type,
		EncryptionKey:        []byte(""),
		InitializationVector: initializationVector,
		CustomAttributes:     metadata,
		Type:                 types.ObjectTypeAppendable,
		StorageClass:         storageClass,
	}

	result.LastModified = object.LastModifiedTime
	result.NextPosition = object.Size
	helper.Logger.Println(20, "Append info.", "bucket:", bucketName, "objName:", objectName, "oid:", oid,
		"objSize:", object.Size, "bytesWritten:", bytesWritten, "storageClass:", storageClass)
	err = yig.MetaStorage.AppendObject(object, isObjectExist(objInfo))
	if err != nil {
		log.Error(fmt.Sprintf("failed to append object %v, err: %v", object, err))
		return
	}

	// update bucket usage.
	err = yig.MetaStorage.UpdateUsage(bucketName, bytesWritten)
	if err != nil {
		log.Error(fmt.Sprintf("failed to update bucket usage for bucket: %s with append object: %v, err: %v", bucketName, object, err))
		return result, err
	}
	yig.MetaStorage.Cache.Remove(redis.ObjectTable, meta.OBJECT_CACHE_PREFIX, bucketName+":"+objectName+":")
	yig.DataCache.Remove(bucketName + ":" + objectName + ":" + object.GetVersionId())
	return result, nil
}*/

// temp comment.
/*func (yig *YigStorage) CopyObject(targetObject *types.Object, source io.Reader, credential common.Credential,
	sseRequest datatype.SseRequest) (result datatype.PutObjectResult, err error) {

	var oid string
	var maybeObjectToRecycle objectToRecycle
	var encryptionKey []byte
	encryptionKey, cipherKey, err := yig.encryptionKeyFromSseRequest(sseRequest, targetObject.BucketName, targetObject.Name)
	if err != nil {
		return
	}

	bucket, err := yig.MetaStorage.GetBucket(targetObject.BucketName, true)
	if err != nil {
		return
	}

	switch bucket.ACL.CannedAcl {
	case "public-read-write":
		break
	default:
		if bucket.OwnerId != credential.UserId {
			return result, ErrBucketAccessForbidden
		}
	}

	// Limit the reader to its provided size if specified.
	var limitedDataReader io.Reader
	limitedDataReader = io.LimitReader(source, targetObject.Size)

	cephCluster, poolName := yig.PickOneClusterAndPool(targetObject.BucketName,
		targetObject.Name, targetObject.Size, false)

	if len(targetObject.Parts) != 0 {
		var targetParts map[int]*types.Part = make(map[int]*types.Part, len(targetObject.Parts))
		//		etaglist := make([]string, len(sourceObject.Parts))
		for partNum, part := range targetObject.Parts {
			targetParts[partNum] = part
			pr, pw := io.Pipe()
			defer pr.Close()
			var total = part.Size
			go func() {
				_, err = io.CopyN(pw, source, total)
				if err != nil {
					return
				}
				pw.Close()
			}()
			md5Writer := md5.New()
			dataReader := io.TeeReader(pr, md5Writer)
			oid = cephCluster.GetUniqUploadName()
			var bytesW int64
			var storageReader io.Reader
			var initializationVector []byte
			if len(encryptionKey) != 0 {
				initializationVector, err = newInitializationVector()
				if err != nil {
					return
				}
			}
			storageReader, err = wrapEncryptionReader(dataReader, encryptionKey, initializationVector)
			bytesW, err = cephCluster.Put(poolName, oid, storageReader)
			maybeObjectToRecycle = objectToRecycle{
				location: cephCluster.Name,
				pool:     poolName,
				objectId: oid,
			}
			if bytesW < part.Size {
				RecycleQueue <- maybeObjectToRecycle
				return result, ErrIncompleteBody
			}
			if err != nil {
				return result, err
			}
			calculatedMd5 := hex.EncodeToString(md5Writer.Sum(nil))
			//we will only chack part etag,overall etag will be same if each part of etag is same
			if calculatedMd5 != part.Etag {
				err = ErrInternalError
				RecycleQueue <- maybeObjectToRecycle
				return
			}
			part.LastModified = time.Now().UTC().Format(types.CREATE_TIME_LAYOUT)
			part.ObjectId = oid

			part.InitializationVector = initializationVector
		}
		targetObject.ObjectId = ""
		targetObject.Parts = targetParts
		result.Md5 = targetObject.Etag
	} else {
		md5Writer := md5.New()

		// Mapping a shorter name for the object
		oid = cephCluster.GetUniqUploadName()
		dataReader := io.TeeReader(limitedDataReader, md5Writer)
		var storageReader io.Reader
		var initializationVector []byte
		if len(encryptionKey) != 0 {
			initializationVector, err = newInitializationVector()
			if err != nil {
				return
			}
		}
		storageReader, err = wrapEncryptionReader(dataReader, encryptionKey, initializationVector)
		if err != nil {
			return
		}
		var bytesWritten int64
		bytesWritten, err = cephCluster.Put(poolName, oid, storageReader)
		if err != nil {
			return
		}
		// Should metadata update failed, add `maybeObjectToRecycle` to `RecycleQueue`,
		// so the object in Ceph could be removed asynchronously
		maybeObjectToRecycle = objectToRecycle{
			location: cephCluster.Name,
			pool:     poolName,
			objectId: oid,
		}
		if bytesWritten < targetObject.Size {
			RecycleQueue <- maybeObjectToRecycle
			return result, ErrIncompleteBody
		}

		calculatedMd5 := hex.EncodeToString(md5Writer.Sum(nil))
		if calculatedMd5 != targetObject.Etag {
			RecycleQueue <- maybeObjectToRecycle
			return result, ErrBadDigest
		}
		result.Md5 = calculatedMd5
		targetObject.ObjectId = oid
		targetObject.InitializationVector = initializationVector
	}
	// TODO validate bucket policy and fancy ACL

	targetObject.Rowkey = nil   // clear the rowkey cache
	targetObject.VersionId = "" // clear the versionId cache
	targetObject.Location = cephCluster.Name
	targetObject.Pool = poolName
	targetObject.OwnerId = credential.UserId
	targetObject.LastModifiedTime = time.Now().UTC()
	targetObject.NullVersion = helper.Ternary(bucket.Versioning == "Enabled", false, true).(bool)
	targetObject.DeleteMarker = false
	targetObject.SseType = sseRequest.Type
	targetObject.EncryptionKey = helper.Ternary(sseRequest.Type == crypto.S3.String(),
		cipherKey, []byte("")).([]byte)

	result.LastModified = targetObject.LastModifiedTime

	var nullVerNum uint64
	nullVerNum, err = yig.checkOldObject(targetObject.BucketName, targetObject.Name, bucket.Versioning)
	if err != nil {
		RecycleQueue <- maybeObjectToRecycle
		return
	}
	if bucket.Versioning == "Enabled" {
		result.VersionId = targetObject.GetVersionId()
	}
	// update null version number
	if bucket.Versioning == "Suspended" {
		nullVerNum = uint64(targetObject.LastModifiedTime.UnixNano())
	}

	objMap := &types.ObjMap{
		Name:       targetObject.Name,
		BucketName: targetObject.BucketName,
	}

	if nullVerNum != 0 {
		objMap.NullVerNum = nullVerNum
		err = yig.MetaStorage.PutObject(targetObject, nil, objMap, true)
	} else {
		err = yig.MetaStorage.PutObject(targetObject, nil, nil, true)
	}

	if err != nil {
		RecycleQueue <- maybeObjectToRecycle
		return
	}

	yig.MetaStorage.Cache.Remove(redis.ObjectTable, meta.OBJECT_CACHE_PREFIX, targetObject.BucketName+":"+targetObject.Name+":")
	yig.DataCache.Remove(targetObject.BucketName + ":" + targetObject.Name + ":" + targetObject.GetVersionId())

	return result, nil
}*/
