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

type SymmetricKeyEncrypter interface {
	Encrypter(plainText string, key []byte) (string, error)
	Decrypter(cipherText string, key []byte) (string, error)
}

func NewSymmetricKeyEncrypter(algo string) SymmetricKeyEncrypter {
	switch algo {
	case "aes":
		return NewAES()
	default:
		return NewAES()
	}
}
