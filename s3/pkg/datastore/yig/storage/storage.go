package storage

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
	"path/filepath"
	"sync"

	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/config"
	"github.com/opensds/multi-cloud/s3/pkg/datastore/yig/crypto"
	"github.com/opensds/multi-cloud/s3/pkg/meta"
	log "github.com/sirupsen/logrus"
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
	Stopping    bool
	WaitGroup   *sync.WaitGroup
}

func New(cfg *config.Config) (*YigStorage, error) {
	kms := crypto.NewKMS()
	yig := YigStorage{
		DataStorage: make(map[string]*CephStorage),
		MetaStorage: meta.New(meta.EnableCache),
		KMS:         kms,
		Stopping:    false,
		WaitGroup:   new(sync.WaitGroup),
	}
	CephConfigPattern := cfg.StorageCfg.CephPath
	if CephConfigPattern == "" {
		CephConfigPattern = DEFAULT_CEPHCONFIG_PATTERN
	}

	cephConfs, err := filepath.Glob(CephConfigPattern)
	log.Infof("Reading Ceph conf files from %+v\n", cephConfs)
	if err != nil || len(cephConfs) == 0 {
		log.Fatal("PANIC: No ceph conf found")
		err = errors.New("no ceph conf found")
		return nil, err
	}

	for _, conf := range cephConfs {
		c := NewCephStorage(conf)
		if c != nil {
			yig.DataStorage[c.Name] = c
		}
	}

	if len(yig.DataStorage) == 0 {
		log.Fatal("PANIC: No data storage can be used!")
		err = errors.New("no working data storage")
		return nil, err
	}

	initializeRecycler(&yig)
	return &yig, nil
}

func (y *YigStorage) Close() error {
	return nil
}

func (y *YigStorage) DriverClose() {
	y.Stopping = true
	log.Info("Stopping storage...")
	y.WaitGroup.Wait()
	log.Info("done")
	log.Info("Stopping MetaStorage...")
	y.MetaStorage.Stop()
}

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
