package keystone

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/opensds/multi-cloud/aksk/pkg/model"
	"github.com/opensds/multi-cloud/aksk/pkg/utils"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
)

const KEYSTONE_URL = "http://192.168.20.108/identity/v3/credentials"

type credblob struct {
	access string
	secret string
}

type reqBody struct {
	Credential credBody
}

type credBody struct {
	blob credblob `json:"blob"`
	projectId string `json:"projectId"`
	userId string `json:"userId"`
	backendtype string `json:"backendtype"`
}


func CreateAKSK( aksk *model.AkSk)(string, error) {
	accessKey, _ := utils.GenerateRandomString(16)
	secretKey, _ := utils.GenerateRandomString(32)

	blob := credblob{access: accessKey, secret:  secretKey}

	credbody := credBody{ blob: blob,
		projectId: "94b280022d0c4401bcf3b0ea85870519",
		userId: "558057c4256545bd8a307c37464003c9",
		backendtype: "ec2"}


	reqbody := reqBody{Credential: credbody}
	mapB3, _ := json.Marshal(reqbody)
	fmt.Println(string(mapB3))


	log.Info("RAJAT - REQUEST - " , string(mapB3))

	akskresp, err := http.Post(KEYSTONE_URL,"application/json",bytes.NewBuffer( mapB3) )

	log.Info("RAJAT - RESPONSE  - " , akskresp)

	defer akskresp.Body.Close()

	if err != nil {
	   return "", err
	}

	return akskresp.Status, nil
}

func ListAKSK() {

}

func DeleteAKSK(id string)(*http.Response, error){

	req, err := http.NewRequest(http.MethodDelete, KEYSTONE_URL+"/"+id, bytes.NewBuffer(nil))
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalln(err)
		return nil, err
	}

	defer resp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(resp.Body)

	// Convert response body to string
	bodyString := string(bodyBytes)
	fmt.Println(bodyString)

	return  resp, nil
}

func GetAKSK()(*http.Response, error) {
// return this.http.get(this.url, param);

	akskresp, err := http.Get(KEYSTONE_URL)
	if err != nil {
		return nil, err
	}

	defer akskresp.Body.Close()
	bodyBytes, _ := ioutil.ReadAll(akskresp.Body)

	// Convert response body to string
	bodyString := string(bodyBytes)
	fmt.Println("API Response as String:\n" + bodyString)

	return akskresp, nil

}
