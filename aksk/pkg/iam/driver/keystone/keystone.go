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
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/opensds/multi-cloud/aksk/pkg/model"
	"github.com/opensds/multi-cloud/aksk/pkg/utils"
	pb "github.com/opensds/multi-cloud/aksk/proto"
)

const (
	PROTOCOL        = "http://"
	AK_LENGTH       = 16
	SK_LENGTH       = 32
	POST            = "POST"
	GET             = "GET"
	DELETE          = "DELETE"
	AUTH_TOKEN      = "X-Auth-Token"
	CONTENT_TYPE    = "Content-Type"
	APPL_JSON       = "application/json"
	USER_QUERY_STR  = "?user_id="
	USER_DETAILS    = "/identity/v3/users/"
	TENANT_DETAILS  = "/identity/v3/projects/"
	SEPERATOR       = "/"
	CREDENTIAL_TYPE = "ec2"
	CREDS_URI       = "/identity/v3/credentials"
)

type KeystoneIam struct {
	Host   string
	URI    string
	Client http.Client
}

type Client struct {
}

var Keystone = &KeystoneIam{}

func Init(host string) *KeystoneIam {
	Keystone.Host = host
	Keystone.URI = CREDS_URI
	return Keystone
}

func (iam *KeystoneIam) CreateAkSk(aksk *model.AkSk, req *pb.AkSkCreateRequest) (*model.Blob, error) {
	akey := utils.GenerateRandomString(AK_LENGTH)
	skey := utils.GenerateRandomString(SK_LENGTH)

	blb := &model.Blob{Access: akey, Secret: skey}
	blbout, err := json.Marshal(blb)
	if err != nil {
		panic(err)
	}

	// Validate UserId, TenantId .
	if len(strings.TrimSpace(aksk.ProjectId)) == 0 || len(strings.TrimSpace(aksk.UserId)) == 0 {
		errMsg := "projectId or userId is empty, please provide valid projectId and userId"
		log.Error(errMsg)
		return nil, errors.New(errMsg)
	}

	// Validate if AKSK is being created for a valid User and Tenant.
	_, err = iam.ValidateId(aksk.ProjectId, req.Token, TENANT_DETAILS)

	if err != nil {
		errMsg := "projectId is not valid, please provide valid projectId "
		log.Error(errMsg)
		return nil, errors.New(errMsg)
	}

	_, err = iam.ValidateId(aksk.UserId, req.Token, USER_DETAILS)
	if err != nil {
		errMsg := "userId is not valid, please provide valid userId "
		log.Error(errMsg)
		return nil, errors.New(errMsg)
	}

	u, err := json.Marshal(model.Credential{Credential: model.CredBody{ProjectId: aksk.ProjectId,
		UserId: aksk.UserId,
		Blob:   string(blbout),
		Type:   CREDENTIAL_TYPE}})
	if err != nil {
		return nil, err
	}

	client := &Keystone.Client
	keystoneURL := PROTOCOL + iam.Host + iam.URI
	postreq, err := http.NewRequest(POST, keystoneURL, bytes.NewBuffer(u))
	if err != nil {
		return nil, err
	}
	postreq.Header.Add(AUTH_TOKEN, req.Token)
	postreq.Header.Set(CONTENT_TYPE, APPL_JSON)

	akskresp, err := client.Do(postreq)
	if err != nil {
		return nil, err
	}
	defer akskresp.Body.Close()

	bodyBytes, _ := ioutil.ReadAll(akskresp.Body)
	var data *model.AkSkOut
	json.Unmarshal(bodyBytes, &data)
	akskStrg := data.Credential.Blob

	var akskBlob *model.Blob
	akskBytes, _ := strconv.Unquote("`" + akskStrg + "`")
	err = json.Unmarshal([]byte(akskBytes), &akskBlob)
	if err != nil {
		return nil, err
	}

	return akskBlob, nil
}

func (iam *KeystoneIam) DeleteAkSk(ctx context.Context, in *pb.DeleteAkSkRequest) error {

	client := &Keystone.Client
	keystoneURL := PROTOCOL + iam.Host + iam.URI
	getreq, err := http.NewRequest(GET, keystoneURL+USER_QUERY_STR+in.UserId, bytes.NewBuffer(nil))
	if err != nil {
		return err
	}
	getreq.Header.Add(AUTH_TOKEN, in.Token)
	getreq.Header.Set(CONTENT_TYPE, APPL_JSON)

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
		delreq, err := http.NewRequest(DELETE, keystoneURL+SEPERATOR+v.ID, bytes.NewBuffer(nil))
		if err != nil {
			return err
		}
		delreq.Header.Add(AUTH_TOKEN, in.Token)
		delreq.Header.Set(CONTENT_TYPE, APPL_JSON)

		delresp, err = client.Do(delreq)
		if err != nil {
			return err
		}
		log.Info("AK SK Delete status :", delresp.Status)
	}

	return nil
}

func (iam *KeystoneIam) GetAkSk(ctx context.Context, in *pb.GetAkSkRequest) (model.Credentials, error) {

	client := &Keystone.Client
	keystoneURL := PROTOCOL + iam.Host + iam.URI
	getreq, err := http.NewRequest(GET, keystoneURL+USER_QUERY_STR+in.UserId, bytes.NewBuffer(nil))
	if err != nil {
		return nil, err
	}
	getreq.Header.Add(AUTH_TOKEN, in.Token)
	getreq.Header.Set(CONTENT_TYPE, APPL_JSON)

	akskresp, err := client.Do(getreq)
	if err != nil {
		return nil, err
	}

	defer akskresp.Body.Close()

	bodyBytes, _ := ioutil.ReadAll(akskresp.Body)
	var akskListout = &model.AkSkListOut{}

	json.Unmarshal(bodyBytes, &akskListout)
	var akskCredListout = model.Credentials{}
	for _, cred := range akskListout.Credentials {
		akskStrg := cred.Blob
		var akskBlob *model.Blob
		akskBytes, _ := strconv.Unquote("`" + akskStrg + "`")
		err = json.Unmarshal([]byte(akskBytes), &akskBlob)
		if err != nil {
			return nil, err
		}

		akskCredListout = append(akskCredListout, model.CredBlob{
			ProjectID: cred.ProjectID,
			Type:      cred.Type,
			UserID:    cred.UserID,
			Blob:      *akskBlob,
		})
	}

	return akskCredListout, nil
}

func (iam *KeystoneIam) DownloadAkSk(ctx context.Context, in *pb.GetAkSkRequest) (*model.AkSkListOut, error) {

	client := &Keystone.Client
	keystoneURL := PROTOCOL + iam.Host + iam.URI
	getreq, err := http.NewRequest(GET, keystoneURL+USER_QUERY_STR+in.UserId, bytes.NewBuffer(nil))
	if err != nil {
		return nil, err
	}
	getreq.Header.Add(AUTH_TOKEN, in.Token)
	getreq.Header.Set(CONTENT_TYPE, APPL_JSON)

	akskresp, err := client.Do(getreq)

	if err != nil {
		return nil, err
	}
	defer akskresp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(akskresp.Body)

	var akskListout = &model.AkSkListOut{}
	json.Unmarshal(bodyBytes, &akskListout)
	return akskListout, nil
}

func (iam *KeystoneIam) ValidateId(id string, token string, uri string) (bool, error) {

	// Validate userId , tenantId are legitimate
	log.Info("validating ID : ", id)
	keystoneURL := PROTOCOL + iam.Host + uri
	getreq, err := http.NewRequest(GET, keystoneURL+id, bytes.NewBuffer(nil))
	errMsg := fmt.Sprintf("error in validating the Id : %v\n", id)
	if err != nil {
		log.Error(errMsg)
		return false, err
	}
	getreq.Header.Add(AUTH_TOKEN, token)
	getreq.Header.Set(CONTENT_TYPE, APPL_JSON)

	var validationResponse *http.Response
	validationResponse, err = iam.Client.Do(getreq)
	if err != nil {
		log.Error(errMsg)
		return false, err
	}
	defer validationResponse.Body.Close()
	log.Info("ValidationResponse ", validationResponse)
	if validationResponse.StatusCode == 200 {
		log.Info("Id is Valid")
		return true, nil
	} else {
		log.Error(errMsg)
		return false, errors.New(errMsg)
	}
}
