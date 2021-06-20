// Copyright 2021 The SODA Authors.
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
	"bytes"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"net/http"
)

const KEYSTONE_URL = "http://192.168.20.108/identity/v3/auth/tokens"



var letters = []rune("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func GenerateRandomString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}


/*func GenerateRandomString(n int) (string, error) {
	const letters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-"
	ret := make([]byte, n)
	for i := 0; i < n; i++ {
		//num, err := cryptorand.Int(cryptorand.Reader, big.NewInt(int64(len(letters))))
		num, err := cryptorand.Int(cryptorand.Reader, big.NewInt(int64(len(letters))))
		if err != nil {
			return "", err
		}
		ret = append(ret, letters[num.Int64()])
	}

	return string(ret), nil
}*/


func GetKeystoneToken() string{
	bodybyt := []byte(
	 `{	"auth": {
		"identity": {
			"methods": [
						"password"
						],
		"password": {
			"user": {	"name": "admin",
						"domain": {
							"name": "Default"
						},
						"password": "opensds@123"
					}
				}
			}
		}
	}`)
	akskresp, err := http.Post(KEYSTONE_URL,"application/json",bytes.NewBuffer(bodybyt))
	log.Info("RAJAT - RESPONSE  - " , akskresp)

	defer akskresp.Body.Close()

	if err != nil {
		return  "Error occurred"
	}

	return akskresp.Header.Get("X-Subject-Token")
}