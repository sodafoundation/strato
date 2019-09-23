package storage

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/config"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/crypto"
	"github.com/opensds/multi-cloud/s3/pkg/helper"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/log"
	"github.com/opensds/multi-cloud/s3/pkg/meta"
)

const (
	AES_BLOCK_SIZE               = 16
	ENCRYPTION_KEY_LENGTH        = 32 // key size for AES-"256"
	INITIALIZATION_VECTOR_LENGTH = 16 // block size of AES
	DEFAULT_CEPHCONFIG_PATTERN   = "conf/*.conf"
)

var (
	RootContext = context.Background()
)

// YigStorage implements StorageDriver
type YigStorage struct {
	DataStorage map[string]*CephStorage
	MetaStorage *meta.Meta
	KMS         crypto.KMS
	logfile     *os.File
	Logger      *log.Logger
	Stopping    bool
	WaitGroup   *sync.WaitGroup
}

func New(cfg *config.Config) (*YigStorage, error) {
	//yig log
	filePath := filepath.Join(cfg.Log.Path, "yig.log")
	logf, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	logger := log.New(logf, fmt.Sprintf("[yig-%s]", cfg.Endpoint.Url), log.LstdFlags, cfg.Log.Level)
	kms := crypto.NewKMS()
	yig := YigStorage{
		DataStorage: make(map[string]*CephStorage),
		MetaStorage: meta.New(logger, meta.EnableCache),
		KMS:         kms,
		logfile:     logf,
		Logger:      logger,
		Stopping:    false,
		WaitGroup:   new(sync.WaitGroup),
	}
	CephConfigPattern := cfg.StorageCfg.CephPath
	if CephConfigPattern == "" {
		CephConfigPattern = DEFAULT_CEPHCONFIG_PATTERN
	}

	cephConfs, err := filepath.Glob(CephConfigPattern)
	logger.Printf(5, "Reading Ceph conf files from %+v\n", cephConfs)
	if err != nil || len(cephConfs) == 0 {
		logger.Printf(0, "PANIC: No ceph conf found")
		err = errors.New("no ceph conf found")
		return nil, err
	}

	for _, conf := range cephConfs {
		c := NewCephStorage(conf, logger)
		if c != nil {
			yig.DataStorage[c.Name] = c
		}
	}

	if len(yig.DataStorage) == 0 {
		logger.Printf(0, "PANIC: No data storage can be used!")
		err = errors.New("no working data storage")
		return nil, err
	}

	initializeRecycler(&yig)
	return &yig, nil
}

func (y *YigStorage) Close() error {
	y.Stopping = true
	helper.Logger.Print(2, "Stopping storage...")
	y.WaitGroup.Wait()
	helper.Logger.Println(2, "done")
	helper.Logger.Print(2, "Stopping MetaStorage...")
	y.MetaStorage.Stop()
	y.logfile.Close()

	return nil
}
/*
func (yig *YigStorage) encryptionKeyFromSseRequest(sseRequest datatype.SseRequest, bucket, object string) (key []byte, encKey []byte, err error) {
	switch sseRequest.Type {
	case "": // no encryption
		return nil, nil, nil
	// not implemented yet
	case crypto.S3KMS.String():
		return nil, nil, ErrNotImplemented
	case crypto.S3.String():
		if yig.KMS == nil {
			return nil, nil, ErrKMSNotConfigured
		}
		key, encKey, err := yig.KMS.GenerateKey(yig.KMS.GetKeyID(), crypto.Context{bucket: path.Join(bucket, object)})
		if err != nil {
			return nil, nil, err
		}
		return key[:], encKey, nil
	case crypto.SSEC.String():
		return sseRequest.SseCustomerKey, nil, nil
	default:
		err = ErrInvalidSseHeader
		return
	}
}
*/
func newInitializationVector() (initializationVector []byte, err error) {

	initializationVector = make([]byte, INITIALIZATION_VECTOR_LENGTH)
	_, err = io.ReadFull(rand.Reader, initializationVector)
	return
}

// Wraps reader with encryption if encryptionKey is not empty
func wrapEncryptionReader(reader io.Reader, encryptionKey []byte,
	initializationVector []byte) (wrappedReader io.Reader, err error) {

	if len(encryptionKey) == 0 {
		return reader, nil
	}

	var block cipher.Block
	block, err = aes.NewCipher(encryptionKey)
	if err != nil {
		return
	}
	stream := cipher.NewCTR(block, initializationVector)
	wrappedReader = cipher.StreamReader{
		S: stream,
		R: reader,
	}
	return
}

type alignedReader struct {
	aligned bool // indicate whether alignment has already been done
	offset  int64
	reader  io.Reader
}

func (r *alignedReader) Read(p []byte) (n int, err error) {
	if r.aligned {
		return r.reader.Read(p)
	}

	r.aligned = true
	buffer := make([]byte, len(p))
	n, err = r.reader.Read(buffer)
	if err != nil {
		return
	}

	n = copy(p, buffer[r.offset:n])
	return
}

// AES is a block cipher with block size of 16 bytes, i.e. the basic unit of encryption/decryption
// is 16 bytes. As an HTTP range request could start from any byte, we need to read one more
// block if necessary.
// Also, our chosen mode of operation for YIG is CTR(counter), which features parallel
// encryption/decryption and random read access. We need all these three features, this leaves
// us only three choices: ECB, CTR, and GCM.
// ECB is best known for its insecurity, meanwhile the GCM implementation of golang(as in 1.7) discourage
// users to encrypt large files in one pass, which requires us to read the whole file into memory. So
// the implement complexity is similar between GCM and CTR, we choose CTR because it's faster(but more
// prone to man-in-the-middle modifications)
//
// See https://en.wikipedia.org/wiki/Block_cipher_mode_of_operation
// and http://stackoverflow.com/questions/39347206
func wrapAlignedEncryptionReader(reader io.Reader, startOffset int64, encryptionKey []byte,
	initializationVector []byte) (wrappedReader io.Reader, err error) {

	if len(encryptionKey) == 0 {
		return reader, nil
	}

	alignedOffset := startOffset / AES_BLOCK_SIZE * AES_BLOCK_SIZE
	newReader, err := wrapEncryptionReader(reader, encryptionKey, initializationVector)
	if err != nil {
		return
	}
	if alignedOffset == startOffset {
		return newReader, nil
	}

	wrappedReader = &alignedReader{
		aligned: false,
		offset:  startOffset - alignedOffset,
		reader:  newReader,
	}
	return
}
