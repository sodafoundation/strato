package blobmover

import (
	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
	pb "github.com/opensds/multi-cloud/datamover/proto"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"net/url"
	"github.com/micro/go-log"
	"context"
	"io"
	"bytes"
	"errors"
	"encoding/binary"
	"encoding/base64"
	"time"
)

var (
	HTTP_OK      = 200
	HTTP_CREATED = 201
)

//TryTimeout indicates the maximum time allowed for any single try of an HTTP request.
var MaxTimeForSingleHttpRequest = 50 * time.Minute

type BlobMover struct {
	containerURL azblob.ContainerURL
	completeParts []string
}

func (mover *BlobMover)Init(endpoint *string, acountName *string, accountKey *string) error {
	var err error
	mover.containerURL, err = mover.createContainerURL(endpoint, acountName, accountKey)
	if err != nil {
		log.Logf("[blobmover] Init container URL faild:%v\n", err)
		return err
	}

	log.Log("[blobmover] Init succeed, container URL:", mover.containerURL.String())
	return nil
}

func (mover *BlobMover)createContainerURL(endpoint *string, acountName *string, accountKey *string) (azblob.ContainerURL,
	error) {
	credential,err := azblob.NewSharedKeyCredential(*acountName, *accountKey)
	if err != nil {
		log.Logf("[blobmover] Create credential failed, err:%v\n", err)
		return azblob.ContainerURL{}, err
	}

	//create containerURL
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{
		Retry: azblob.RetryOptions{
			TryTimeout: MaxTimeForSingleHttpRequest,
		},
	})
	URL, _ := url.Parse(*endpoint)

	return azblob.NewContainerURL(*URL, p), nil
}

func (mover *BlobMover)DownloadObj(objKey string, srcLoca *LocationInfo, buf []byte) (size int64, err error){
	err = mover.Init(&srcLoca.EndPoint, &srcLoca.Access, &srcLoca.Security)
	if err != nil {
		return 0, err
	}

	log.Logf("[blobmover] Try to download, bucket:%s,obj:%s\n", srcLoca.BucketName, objKey)
	ctx := context.Background()
	blobURL := mover.containerURL.NewBlockBlobURL(objKey)
	for tries := 1; tries <= 3; tries++ {
		downloadResp, err := blobURL.Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{},
			false)
		if err != nil {
			log.Logf("[blobmover] Download object[%s] faild %d times, err:%v\n", objKey, tries, err)
			if tries == 3 {
				return 0, err
			}
		} else {
			size = 0
			var readErr error
			var readCount int = 0
			for {
				s := buf[size:]
				readCount, readErr = downloadResp.Response().Body.Read(s)
				if readCount > 0 {
					size += int64(readCount)
				}
				if readErr != nil {
					log.Logf("[blobmover] readErr[objkey:%s]=%v\n", objKey, readErr)
					break
				}
			}
			if readErr == io.EOF {
				readErr = nil
			}
			log.Logf("[blobmover] Download object[%s] successfully.", objKey)
			return size, readErr
		}
	}

	log.Logf("[blobmover] Download object[%s], should not be here.", objKey)
	return 0, errors.New("internal error")
}

func (mover *BlobMover)UploadObj(objKey string, destLoca *LocationInfo, buf []byte) error {
	err := mover.Init(&destLoca.EndPoint, &destLoca.Access, &destLoca.Security)
	if err != nil {
		return err
	}

	ctx := context.Background()
	blobURL := mover.containerURL.NewBlockBlobURL(objKey)
	log.Logf("[blobmover] Try to upload object[%s].", objKey)
	for tries := 1; tries <= 3; tries++ {
		uploadResp, err := blobURL.Upload(ctx, bytes.NewReader(buf), azblob.BlobHTTPHeaders{}, nil,
			azblob.BlobAccessConditions{})
		if err != nil {
			log.Logf("[blobmover] Upload object[%s] faild %d times, err:%v\n", objKey, tries, err)
			if tries == 3 {
				return err
			}
		} else if uploadResp.StatusCode() != HTTP_CREATED {
			log.Logf("[blobmover] Upload object[%s] StatusCode:%d\n", objKey, uploadResp.StatusCode())
			if tries == 3 {
				return errors.New("Upload failed")
			}
		} else {
			log.Logf("[blobmover] Upload object[%s] successfully.", objKey)
			return nil
		}
	}

	log.Logf("[blobmover] Upload object[%s], should not be here.", objKey)
	return errors.New("internal error")
}

func (mover *BlobMover)DeleteObj(objKey string, loca *LocationInfo) error {
	err := mover.Init(&loca.EndPoint, &loca.Access, &loca.Security)
	if err != nil {
		return err
	}

	ctx := context.Background()
	blobURL := mover.containerURL.NewBlockBlobURL(objKey)
	log.Logf("[blobmover] Try to delete object[%s].", objKey)
	for tries := 1; tries <= 3; tries++ {
		delRsp, err := blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{})
		if err != nil {
			log.Logf("[blobmover] Delete object[%s] faild %d times, err:%v\n", objKey, tries, err)
			if tries == 3 {
				return err
			}
		} else if delRsp.StatusCode() != HTTP_OK {
			log.Logf("[blobmover] Delete object[%s] StatusCode:%d\n", objKey, delRsp.StatusCode())
			if tries == 3 {
				return errors.New("Delete failed")
			}
		} else {
			log.Logf("[blobmover] Delete object[%s] successfully.", objKey)
			return nil
		}
	}

	log.Logf("[blobmover] Delete object[%s], should not be here.", objKey)
	return errors.New("internal error")
}

func (mover *BlobMover)MultiPartDownloadInit(srcLoca *LocationInfo) error {
	log.Logf("[blobmover] Prepare to do part upload, container:%s.\n", srcLoca.BucketName)

	return mover.Init(&srcLoca.EndPoint, &srcLoca.Access, &srcLoca.Security)
}

func (mover *BlobMover)DownloadRange(objKey string, srcLoca *LocationInfo, buf []byte, start int64, end int64) (size int64,
	err error) {
	log.Logf("[blobmover] Try to download object[%s] range[%d - %d]...\n", objKey, start, end)

	ctx := context.Background()
	blobURL := mover.containerURL.NewBlobURL(objKey)
	count := end - start + 1

	for tries := 1; tries <= 3; tries++ {
		err = azblob.DownloadBlobToBuffer(ctx, blobURL, start, count, buf, azblob.DownloadFromBlobOptions{})
		if err != nil {
			log.Logf("[blobomver] Donwload object[%s] to buffer failed %d times, err:%v\n", objKey, tries, err)
			if tries == 3 {
				return 0,err
			}
		} else {
			log.Logf("[blobmover] Download object[%s] range[%d - %d] successfully.\n", objKey, start, end)
			return count, nil
		}
	}

	log.Logf("[blobmover] Download object[%s] range[%d - %d], should not be here.\n", objKey, start, end)
	return 0,errors.New("internal error")
}

func (mover *BlobMover)MultiPartUploadInit(objKey string, destLoca *LocationInfo) error {
	log.Logf("[blobmover] Prepare to do part upload for object[%s], container:%s, blob:%s\n",
		objKey, destLoca.BucketName, objKey)

	return mover.Init(&destLoca.EndPoint, &destLoca.Access, &destLoca.Security)
}

func (mover *BlobMover)Int64ToBase64(blockID int64) string {
	buf := (&[8]byte{})[:]
	binary.LittleEndian.PutUint64(buf, uint64(blockID))
	return mover.BinaryToBase64(buf)
}

func (mover *BlobMover)BinaryToBase64(binaryID []byte) string {
	return base64.StdEncoding.EncodeToString(binaryID)
}

func (mover *BlobMover)Base64ToInt64(base64ID string) int64 {
	bin, _ := base64.StdEncoding.DecodeString(base64ID);
	return int64(binary.LittleEndian.Uint64(bin))
}

func (mover *BlobMover)UploadPart(objKey string, destLoca *LocationInfo, upBytes int64, buf []byte, partNumber int64,
	offset int64) error {
	log.Logf("[blobmover] Try to upload object[%s] range[partnumber#%d,offset#%d]...\n", objKey, partNumber, offset)
	//TODO: Consider that "A blob can have up to 100,000 uncommitted blocks, but their total size cannot exceed 200,000 MB."

	ctx := context.Background()
	blobURL := mover.containerURL.NewBlockBlobURL(objKey)
	base64ID := mover.Int64ToBase64(partNumber)
	for tries := 1; tries <= 3; tries++ {
		_, err := blobURL.StageBlock(ctx, base64ID, bytes.NewReader(buf), azblob.LeaseAccessConditions{}, nil)
		if err != nil {
			log.Logf("[blobmover] Upload object[objkey:%s] part[%d] failed %d times. err:%v\n", objKey, partNumber, tries, err)
			if tries == 3 {
				return err
			}
		} else {
			log.Logf("[blobmover] Upload range[objkey:%s, partnumber#%d, base64ID#%d] successfully.\n",
				objKey, partNumber, base64ID)
			mover.completeParts = append(mover.completeParts, base64ID)
			return nil
		}
	}

	log.Logf("[blobmover] Upload range[objkey:%s, partnumber#%d, base64ID#%d], should not be here.\n",
		objKey, partNumber, base64ID)
	return errors.New("internal error")
}

func (mover *BlobMover)AbortMultipartUpload(objKey string, destLoca *LocationInfo) error {
	log.Logf("No need to abort multipart upload[objkey:%s].\n", objKey)
	return nil
}

//A blob can have up to 100,000 uncommitted blocks, but their total size cannot exceed 200,000 MB.
func (mover *BlobMover)CompleteMultipartUpload(objKey string, destLoca *LocationInfo) error {
	ctx := context.Background()
	blobURL := mover.containerURL.NewBlockBlobURL(objKey)

	log.Logf("[blobmover] Try to CompleteMultipartUpload of object[%s].\n", objKey)
	for tries := 1; tries <= 3; tries++ {
		_, err := blobURL.CommitBlockList(ctx, mover.completeParts, azblob.BlobHTTPHeaders{}, azblob.Metadata{}, azblob.BlobAccessConditions{})
		if err != nil {
			log.Logf("[blobmover] CompleteMultipartUpload of object[%s] failed:%v\n", objKey, err)
			if tries == 3 {
				return err
			}
		} else {
			log.Logf("[blobmover] CompleteMultipartUpload of object[%s] successfully.\n", objKey)
			return nil
		}
	}

	log.Logf("[blobmover] CompleteMultipartUpload of object[%s], should not be here.\n", objKey)
	return errors.New("internal error")
}

func ListObjs(loca *LocationInfo, filt *pb.Filter) ([]azblob.BlobItem, error) {
	log.Logf("[blobmover] List objects of container[%s]\n", loca.BucketName)
	credential,err := azblob.NewSharedKeyCredential(loca.Access, loca.Security)
	if err != nil {
		log.Fatalf("[blobmover] Create credential failed for list objects, err:%v\n", err)
		return nil, err
	}

	//create containerURL
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	URL, _ := url.Parse(loca.EndPoint)
	containerURL := azblob.NewContainerURL(*URL, p)

	//TODO: Set the best context
	ctx := context.Background()

	var objs []azblob.BlobItem
	option := azblob.ListBlobsSegmentOptions{}
	if filt != nil {
		option.Prefix = filt.Prefix
	}
	for marker := (azblob.Marker{}); marker.NotDone(); {
		// Get a result segment starting with the blob indicated by the current Marker.
		listBlob, err := containerURL.ListBlobsFlatSegment(ctx, marker, option)
		if err != nil {
			log.Logf("[blobmover] ListBlobsFlatSegment failed:%v\n", err)
			return nil, err
		}
		objs = append(objs, listBlob.Segment.BlobItems...)

		marker = listBlob.NextMarker
	}

	return objs, nil
}
