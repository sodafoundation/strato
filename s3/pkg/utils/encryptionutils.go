package utils

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"
)
import log "github.com/sirupsen/logrus"

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
func WrapAlignedEncryptionReader(reader io.Reader, startOffset int64, encryptionKey []byte,
	initializationVector []byte) (wrappedReader io.Reader, err error) {

	if len(encryptionKey) == 0 {
		return reader, nil
	}

	alignedOffset := startOffset / 16 * 16
	newReader, err := WrapEncryptionReader(reader, encryptionKey, initializationVector)
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

// Wraps reader with encryption if encryptionKey is not empty
func WrapEncryptionReader(reader io.Reader, encryptionKey []byte,
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

func EncryptWithAES256RandomKey(data []byte, key []byte) (error, []byte) {
	// use the key, get the cipher
	cipherBlock, cipherErr := aes.NewCipher(key)
	if cipherErr != nil {
		log.Errorf("Encryption error, cipher not generated")
		return cipherErr, nil
	}
	// use the cipher block to encrypt
	// gcm or Galois/Counter Mode, is a mode of operation
	// for symmetric key cryptographic block ciphers
	// https://en.wikipedia.org/wiki/Galois/Counter_Mode
	aesgcm, gcmErr := cipher.NewGCM(cipherBlock)
	if gcmErr != nil {
		log.Errorf("Encryption error, GCM not generated")
		return gcmErr, nil
	}
	// NonceSize is default 12 bytes
	nonce := make([]byte, aesgcm.NonceSize())
	_, nonceErr := io.ReadFull(rand.Reader, nonce)
	if nonceErr != nil {
		log.Errorf("Encryption error, GCM nonce not created")
		return nonceErr, nil
	}

	// use the aes gcm to seal the data
	encBytes := aesgcm.Seal(nonce, nonce, data, nil)
	return nil, encBytes
}

func DecryptWithAES256(data []byte, key []byte) (error, []byte) {
	// use the key, get the cipher
	cipherBlock, cipherErr := aes.NewCipher(key)
	if cipherErr != nil {
		log.Errorf("Decryption error, cipher not generated")
		return cipherErr, nil
	}
	// use the cipher block to decrypt
	// gcm or Galois/Counter Mode, is a mode of operation
	// for symmetric key cryptographic block ciphers
	// https://en.wikipedia.org/wiki/Galois/Counter_Mode
	aesgcm, gcmErr := cipher.NewGCM(cipherBlock)
	if gcmErr != nil {
		log.Errorf("Decryption error, GCM not generated")
		return gcmErr, nil
	}
	nonceSize := aesgcm.NonceSize()
	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	// use the aes gcm to open the data
	decBytes, decErr := aesgcm.Open(nil, nonce, ciphertext, nil)
	if decErr != nil {
		log.Errorf("Decryption error during open %s", decErr)
		return decErr, nil
	}
	return decErr, decBytes
}

func GetRandomNBitKey(numBits int) ([]byte, error) {
	key := make([]byte, numBits)
	_, err := io.ReadFull(rand.Reader, key)
	if err != nil {
		log.Errorf("Error generating random %d bit key %s", numBits, err)
		return nil, err
	}
	log.Infof("Generated random %d bit key", numBits)
	return key, nil
}
