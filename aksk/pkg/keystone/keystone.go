package keystone

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/opensds/multi-cloud/aksk/pkg/model"
	"github.com/opensds/multi-cloud/aksk/pkg/utils"
	pb "github.com/opensds/multi-cloud/aksk/proto"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
)

const KEYSTONE_URL = "http://192.168.20.108/identity/v3/credentials"

type blob struct {
	Access string `json:"access"`
	Secret string `json:"secret"`
}


type CredBody struct {
	ProjectId string `json:"project_id"`
	UserId string `json:"user_id"`
	Blob string `json:"blob"`
	Type string `json:"type"`
}

type Credential struct {
	Credential CredBody `json:"credential"`
}

type Client struct {

}

func CreateAKSK(aksk *model.AkSk, req *pb.CreateAkSkRequest) (*http.Response, error) {

	akey:= utils.GenerateRandomString(16)
	skey := utils.GenerateRandomString(32)

	blb := blob{Access: akey, Secret: skey}
	log.Info("RAJAT - AK SK  - " , akey , skey)

	byts, err := json.Marshal(blb)
	if err != nil {
		panic(err)
	}

	in := string(byts)
	byts, err = json.Marshal(in)
	if err != nil {
		panic(err)
	}

	//blobbody , err :=  json.Marshal(blob{access: akey, secret: skey})
	u, err := json.Marshal(Credential{Credential: CredBody{ProjectId: aksk.ProjectId,
		UserId: aksk.UserId,
		Blob: in,
		Type: "ec2"}})

	fmt.Println(string(u))
	client := &http.Client{}

	postreq , err := http.NewRequest("POST", KEYSTONE_URL, bytes.NewBuffer(u))
	postreq.Header.Add("X-Auth-Token", req.Aksk.Token)
	postreq.Header.Set("Content-Type", "application/json")
	postreq.Header.Set("Accept", "*/*")
	postreq.Header.Set("Accept-Encoding"," gzip, deflate, br")

	akskresp, _ := client.Do(postreq)
	log.Info("RAJAT - RESPONSE  - " , akskresp)

	defer akskresp.Body.Close()

	if err != nil {
	   return nil, err
	}

	log.Info("RAJAT - Create AKSK Response - ", akskresp)
	return akskresp, nil
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
