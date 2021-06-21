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

package keystone

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/opensds/multi-cloud/aksk/pkg/iam"

	"io/ioutil"
	"net/http"

	"github.com/opensds/multi-cloud/aksk/pkg/model"
	"github.com/opensds/multi-cloud/aksk/pkg/utils"
	pb "github.com/opensds/multi-cloud/aksk/proto"
	_ "github.com/sirupsen/logrus"
)

func (iam *keystoneIam) Close() {
	panic("Not used as of now!!")
}

const KEYSTONE_URL = "http://192.168.20.108/identity/v3/credentials"

type blob struct {
	Access string `json:"access"`
	Secret string `json:"secret"`
}

type CredBody struct {
	ProjectId string `json:"project_id"`
	UserId    string `json:"user_id"`
	Blob      string `json:"blob"`
	Type      string `json:"type"`
}

type Credential struct {
	Credential CredBody `json:"credential"`
}

type Client struct {
}

type keystoneIam struct {
}

var keystone = &keystoneIam{}

func Init(host string) iam.IAMAuthenticator {
	return keystone
}

func (iam *keystoneIam) CreateAkSk(aksk *model.AkSk, req *pb.CreateAkSkRequest) (*model.AkSkOut, error) {
	akey := utils.GenerateRandomString(16)
	skey := utils.GenerateRandomString(32)

	blb := &blob{Access: akey, Secret: skey}
	blbout, err := json.Marshal(blb)
	if err != nil {
		panic(err)
	}

	u, err := json.Marshal(Credential{Credential: CredBody{ProjectId: aksk.ProjectId,
		UserId: aksk.UserId,
		Blob:   string(blbout),
		Type:   "ec2"}})

	client := &http.Client{}
	postreq, err := http.NewRequest("POST", KEYSTONE_URL, bytes.NewBuffer(u))
	postreq.Header.Add("X-Auth-Token", req.Aksk.Token)
	postreq.Header.Set("Content-Type", "application/json")

	akskresp, _ := client.Do(postreq)
	defer akskresp.Body.Close()

	if err != nil {
		return nil, err
	}

	bodyBytes, _ := ioutil.ReadAll(akskresp.Body)

	var data *model.AkSkOut
	json.Unmarshal(bodyBytes, &data)

	return data, nil
}

func (iam *keystoneIam) DeleteAkSk(ctx context.Context, in *pb.DeleteAkSkRequest) error {

	client := &http.Client{}
	getreq, err := http.NewRequest("DELETE", KEYSTONE_URL+"/"+in.GetId(), bytes.NewBuffer(nil))
	getreq.Header.Add("X-Auth-Token", in.AkSkDetail.Token)
	getreq.Header.Set("Content-Type", "application/json")

	akskresp, err := client.Do(getreq)
	if err != nil {
		return err
	}
	//bodyBytes, _ := ioutil.ReadAll(akskresp.Body)

	defer akskresp.Body.Close()
	return nil
}

func (iam *keystoneIam) GetAkSk(ctx context.Context, in *pb.GetAkSkRequest) (*model.AkSkListOut, error) {

	client := &http.Client{}
	getreq, err := http.NewRequest("GET", KEYSTONE_URL+"?user_id="+in.AkSkDetail.UserId, bytes.NewBuffer(nil))
	getreq.Header.Add("X-Auth-Token", in.AkSkDetail.Token)
	getreq.Header.Set("Content-Type", "application/json")

	akskresp, err := client.Do(getreq)
	defer akskresp.Body.Close()

	if err != nil {
		return nil, err
	}

	bodyBytes, _ := ioutil.ReadAll(akskresp.Body)
	var akskListout = &model.AkSkListOut{}
	json.Unmarshal(bodyBytes, &akskListout)

	return akskListout, nil
}

func (iam *keystoneIam) DownloadAkSk(ctx context.Context, in *pb.GetAkSkRequest) (*model.AkSkListOut, error) {
	client := &http.Client{}
	getreq, err := http.NewRequest("GET", KEYSTONE_URL+"?user_id="+in.AkSkDetail.UserId, bytes.NewBuffer(nil))
	getreq.Header.Add("X-Auth-Token", in.AkSkDetail.Token)
	getreq.Header.Set("Content-Type", "application/json")

	akskresp, err := client.Do(getreq)
	defer akskresp.Body.Close()

	if err != nil {
		return nil, err
	}
	bodyBytes, _ := ioutil.ReadAll(akskresp.Body)

	// Convert response body to string
	var akskListout = &model.AkSkListOut{}
	json.Unmarshal(bodyBytes, &akskListout)
	return akskListout, nil
}
