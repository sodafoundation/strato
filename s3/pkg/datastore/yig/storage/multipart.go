package storage

/*import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net/url"
	"sort"
	"strconv"
	"time"

	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/common"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/crypto"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/datatype"
	. "github.com/opensds/multi-cloud/s3/pkg/datastore/yig/error"
	"github.com/opensds/multi-cloud/s3/pkg/helper"
	obj "github.com/opensds/multi-cloud/s3/pkg/datastore/yig/meta"
	meta "github.com/opensds/multi-cloud/s3/pkg/datastore/yig/meta/types"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/redis"
)*/

import (
	"context"
	"errors"
	"io"

	"github.com/opensds/multi-cloud/s3/pkg/model"
	pb "github.com/opensds/multi-cloud/s3/proto"
)

const (
	MAX_PART_SIZE   = 5 << 30 // 5GB
	MAX_PART_NUMBER = 10000
)

func (yig *YigStorage) InitMultipartUpload(ctx context.Context, object *pb.Object) (*pb.MultipartUpload, error) {
	return nil, errors.New("not implemented.")
}

func (yig *YigStorage) UploadPart(ctx context.Context, stream io.Reader, multipartUpload *pb.MultipartUpload,
	partNumber int64, upBytes int64) (*model.UploadPartResult, error) {
	return nil, errors.New("not implemented.")
}

func (yig *YigStorage) CompleteMultipartUpload(ctx context.Context, multipartUpload *pb.MultipartUpload,
	completeUpload *model.CompleteMultipartUpload) (*model.CompleteMultipartUploadResult, error) {
	return nil, errors.New("not implemented.")
}

func (yig *YigStorage) AbortMultipartUpload(ctx context.Context, multipartUpload *pb.MultipartUpload) error {
	return errors.New("not implemented.")
}
