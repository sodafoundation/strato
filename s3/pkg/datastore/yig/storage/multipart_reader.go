package storage

import (
	"io"

	s3err "github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/meta/types"
	log "github.com/sirupsen/logrus"
)

type MultipartReader struct {
	// uploadId for this multipart uploaded object.
	uploadId uint64
	// where to read from
	start int64
	// how much data to read
	len int64
	// parts for this uploadId, must be sorted by part number ascendly.
	parts []*types.PartInfo
	// YigStorage handle.
	yig *YigStorage
	// reader from ceph cluster.
	reader io.ReadCloser
}

func (mr *MultipartReader) Read(p []byte) (n int, err error) {
	if len(mr.parts) == 0 || mr.len <= 0 {
		return 0, io.EOF
	}
	if mr.parts[len(mr.parts)-1].Offset+mr.parts[len(mr.parts)-1].Size <= uint64(mr.start) {
		return 0, io.EOF
	}
	// total length of input buffer
	total := len(p)
	if int64(total) > mr.len {
		total = int(mr.len)
	}
	// where to start read from in the buffer.
	// the data whose position < begin is already read.
	begin := 0
	for _, part := range mr.parts {
		if part.Offset+part.Size < uint64(mr.start) {
			continue
		}
		cephCluster, ok := mr.yig.DataStorage[part.Location]
		if !ok {
			log.Errorf("failed to get cephCluster for part(%d, %d, %s)", part.UploadId, part.PartNum, part.Location)
			return 0, s3err.ErrInvalidPart
		}
		reader, err := cephCluster.getReader(part.Pool, part.ObjectId, mr.start-int64(part.Offset), int64(total-begin))
		if err != nil {
			log.Errorf("failed to get reader from ceph cluter for part(%d, %d, %s), err: %v", part.UploadId, part.PartNum, part.Location)
			return 0, s3err.ErrInvalidPart
		}
		n, err := reader.Read(p[begin:])
		// one read will try to read the whole data for the input buffer on each reader.
		reader.Close()
		if err != nil {
			if err == io.EOF {
				mr.start += int64(n)
				begin += n
				mr.len -= int64(n)
				if begin < total {
					continue
				}
				return 0, err
			}
			log.Infof("read data from uploadId(%d) with start(%d), len(%d) failed, err: %v", mr.uploadId, mr.start, mr.len, err)
			return n, err
		}
		mr.start += int64(n)
		begin += n
		mr.len -= int64(n)
		if begin >= total {
			break
		}
	}

	return begin, nil
}

func (mr *MultipartReader) Close() error {
	if mr.reader != nil {
		return mr.reader.Close()
	}

	return nil
}

func NewMultipartReader(yig *YigStorage, uploadIdStr string, start int64, end int64) (*MultipartReader, error) {
	uploadId, err := str2UploadId(uploadIdStr)
	if err != nil {
		log.Errorf("failed to create MultipartReader, got invalid uploadId(%s), err: %v", uploadIdStr, err)
		return nil, err
	}
	totalparts, err := yig.MetaStorage.ListParts(uploadId)
	if err != nil {
		log.Errorf("failed to create MultipartReader, failed to list parts for uploadId(%s), err: %v", uploadIdStr, err)
		return nil, err
	}
	var parts []*types.PartInfo
	for _, part := range totalparts {
		if part.Offset+part.Size >= uint64(start) && part.Offset <= uint64(end) {
			parts = append(parts, part)
		}
	}
	return &MultipartReader{
		uploadId: uploadId,
		start:    start,
		len:      end - start + 1,
		parts:    parts,
		yig:      yig,
	}, nil
}
