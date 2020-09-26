// Copyright 2020 The SODA Authors.
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

package cryptography

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
)

type AES struct{}

func NewAES() *AES {
	return &AES{}
}

func (*AES) Encrypter(password string, key []byte) (string, error) {
	if len(key) != 16 && len(key) != 24 && len(key) != 32{
		return "", errors.New("length of the key must be either 16, 24, or 32")
	}

	plaintext := []byte(password)

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	cipherText := make([]byte, aes.BlockSize+len(plaintext))
	data := cipherText[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, data); err != nil {
		return "", err
	}

	stream := cipher.NewCFBEncrypter(block, data)
	stream.XORKeyStream(cipherText[aes.BlockSize:], plaintext)

	return hex.EncodeToString(cipherText), nil
}

func (*AES) Decrypter(code string, key []byte) (string, error) {
	cipherText, err := hex.DecodeString(code)
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	if len(cipherText) < aes.BlockSize {
		return "", errors.New("cipherText too short")
	}

	data := cipherText[:aes.BlockSize]
	cipherText = cipherText[aes.BlockSize:]

	stream := cipher.NewCFBDecrypter(block, data)
	stream.XORKeyStream(cipherText, cipherText)

	return string(cipherText), nil
}
