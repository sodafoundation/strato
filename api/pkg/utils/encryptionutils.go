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

package utils

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"

	log "github.com/sirupsen/logrus"
)

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
	nonce := make([]byte, aesgcm.NonceSize())
	_, nonceErr := io.ReadFull(rand.Reader, nonce)
	if nonceErr != nil {
		log.Errorf("Encryption error, GCM nonce not createdf")
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

func GetRandom32BitKey() ([]byte, error) {
	key := make([]byte, 32)

	_, err := rand.Read(key)
	if err != nil {
		log.Errorf("Error generating random 32 bit key %s", err)
		return nil, err
	}
	log.Infof("Generated random 32 bit key")
	return key, nil
}
