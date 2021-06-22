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
	"io/ioutil"
	"net/http"

	"github.com/opensds/multi-cloud/aksk/pkg/model"
	"github.com/opensds/multi-cloud/aksk/pkg/utils"
	pb "github.com/opensds/multi-cloud/aksk/proto"

	log "github.com/sirupsen/logrus"
)

const (
	KEYSTONE_URI = "/identity/v3/credentials"
	PROTOCOL = "http://"
	AK_LENGTH = 16
	SK_LENGTH = 32
)

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
	host string
	uri  string
}

var keystone = &keystoneIam{}

func Init(host string) *keystoneIam {
	keystone.host = host
	return keystone
}

func (iam *keystoneIam) CreateAkSk(aksk *model.AkSk, req *pb.CreateAkSkRequest) (*model.AkSkOut, error) {
	akey := utils.GenerateRandomString(AK_LENGTH)
	skey := utils.GenerateRandomString(SK_LENGTH)

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
	keystoneURL := PROTOCOL + iam.host + KEYSTONE_URI
	postreq, err := http.NewRequest("POST", keystoneURL, bytes.NewBuffer(u))
	if err != nil {
		return nil, err
	}
	postreq.Header.Add("X-Auth-Token", req.Aksk.Token)
	postreq.Header.Set("Content-Type", "application/json")

	akskresp, _ := client.Do(postreq)
	defer akskresp.Body.Close()

	bodyBytes, _ := ioutil.ReadAll(akskresp.Body)
	var data *model.AkSkOut
	json.Unmarshal(bodyBytes, &data)

	return data, nil
}

func (iam *keystoneIam) DeleteAkSk(ctx context.Context, in *pb.DeleteAkSkRequest) error {

	client := &http.Client{}
	keystoneURL := PROTOCOL + iam.host + KEYSTONE_URI
	getreq, err := http.NewRequest("GET", keystoneURL+"?user_id="+in.AkSkDetail.UserId, bytes.NewBuffer(nil))
	getreq.Header.Add("X-Auth-Token", in.AkSkDetail.Token)
	getreq.Header.Set("Content-Type", "application/json")

	akskresp, err := client.Do(getreq)
	if err != nil {
		return err
	}
	defer akskresp.Body.Close()

	bodyBytes, _ := ioutil.ReadAll(akskresp.Body)
	var akskListout = &model.AkSkListOut{}
	err = json.Unmarshal(bodyBytes, &akskListout)
	if err != nil {
		return err
	}

	var delresp *http.Response
	for _, v := range akskListout.Credentials {
		log.Info("Deleting AK SK for User %s", v.ID)
		delreq, err := http.NewRequest("DELETE", keystoneURL+"/"+v.ID, bytes.NewBuffer(nil))
		delreq.Header.Add("X-Auth-Token", in.AkSkDetail.Token)
		delreq.Header.Set("Content-Type", "application/json")

		delresp, err = client.Do(delreq)
		if err != nil {
			return err
		}
		log.Info("AK SK Deleted for user :  RESPONSE", delresp)
	}

	return nil
}

func (iam *keystoneIam) GetAkSk(ctx context.Context, in *pb.GetAkSkRequest) (*model.AkSkListOut, error) {

	client := &http.Client{}
	keystoneURL := PROTOCOL + iam.host + KEYSTONE_URI
	getreq, err := http.NewRequest("GET", keystoneURL+"?user_id="+in.AkSkDetail.UserId, bytes.NewBuffer(nil))
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
	keystoneURL := PROTOCOL + iam.host + KEYSTONE_URI
	getreq, err := http.NewRequest("GET", keystoneURL+"?user_id="+in.AkSkDetail.UserId, bytes.NewBuffer(nil))
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
