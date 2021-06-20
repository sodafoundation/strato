package keystone

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	//"github.com/opensds/multi-cloud/aksk/pkg/iam"
	"github.com/opensds/multi-cloud/aksk/pkg/model"
	"github.com/opensds/multi-cloud/aksk/pkg/utils"
	pb "github.com/opensds/multi-cloud/aksk/proto"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
)




func (iam *keystoneIam) Close() {
	panic("Not used!!")
}

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

type keystoneIam struct {
}

var keystone = &keystoneIam{}

func Init(host string) *keystoneIam {
	return keystone
}


func (iam *keystoneIam) CreateAkSk(aksk *model.AkSk, req *pb.CreateAkSkRequest) (*model.AkSk, error) {
	akey:= utils.GenerateRandomString(16)
	skey := utils.GenerateRandomString(32)

	blb := blob{Access: akey, Secret: skey}
	log.Info("RAJAT - AK SK  - " , akey , skey)

	byts, err := json.Marshal(blb)
	if err != nil {
		panic(err)
	}

	blobin := string(byts)
	byts, err = json.Marshal(blobin)
	if err != nil {
		panic(err)
	}

	u, err := json.Marshal(Credential{Credential: CredBody{ProjectId: aksk.ProjectId,
		UserId: aksk.UserId,
		Blob: blobin,
		Type: "ec2"}})

	fmt.Println(string(u))
	client := &http.Client{}

	postreq , err := http.NewRequest("POST", KEYSTONE_URL, bytes.NewBuffer(u))
	postreq.Header.Add("X-Auth-Token", req.Aksk.Token)
	postreq.Header.Set("Content-Type", "application/json")

	akskresp, _ := client.Do(postreq)
	log.Info("RAJAT - RESPONSE  - " , akskresp)

	defer akskresp.Body.Close()

	if err != nil {
	   return nil, err
	}

	log.Info("RAJAT - Create AKSK Response - ", akskresp)
	return aksk, nil
}

/*func (iam *keystoneIam) ListAKSK(ctx context.Context)([]*model.AkSk, error) {

	postreq , err := http.NewRequest("GET", KEYSTONE_URL, bytes.NewBuffer(nil))
	postreq.Header.Add("X-Auth-Token", req.Aksk.Token)
	postreq.Header.Set("Content-Type", "application/json")

	akskresp, err := http.Get(KEYSTONE_URL)
	if err != nil {
		return nil, err
	}

	bodyBytes, _ := ioutil.ReadAll(akskresp.Body)

	// Convert response body to string
	bodyString := string(bodyBytes)
	fmt.Println("API Response as String:\n" + bodyString)
	defer akskresp.Body.Close()
	return akskresp, nil
}*/


func (iam *keystoneIam) DeleteAkSk(ctx context.Context, in *pb.DeleteAkSkRequest) error {

	client := &http.Client{}
	log.Info("RAJAT - DELETE  - URL " , KEYSTONE_URL+"/" + in.GetId())
	getreq , err := http.NewRequest("DELETE", KEYSTONE_URL+"/" + in.GetId(), bytes.NewBuffer(nil))
	getreq.Header.Add("X-Auth-Token", in.AkSkDetail.Token)
	getreq.Header.Set("Content-Type", "application/json")

	akskresp, err := client.Do(getreq)
	log.Info("RAJAT - RESPONSE  - " , akskresp)

	if err != nil {
		return  err
	}

	log.Info("RAJAT - Delete AKSK Response - ", akskresp)
	bodyBytes, _ := ioutil.ReadAll(akskresp.Body)

	// Convert response body to string
	bodyString := string(bodyBytes)
	fmt.Println("AKSK API Response as String:\n" + bodyString)
	//defer akskresp.Body.Close()
	return  nil
}


func (iam *keystoneIam) GetAkSk(ctx context.Context, in *pb.GetAkSkRequest) (*model.GetAkSk, error) {

	client := &http.Client{}

	getreq , err := http.NewRequest("GET", KEYSTONE_URL + "/" + in.GetId(), bytes.NewBuffer(nil))
	getreq.Header.Add("X-Auth-Token", in.AkSkDetail.Token)
	getreq.Header.Set("Content-Type", "application/json")

	akskresp, err := client.Do(getreq)
	log.Info("RAJAT - RESPONSE  - " , akskresp)

	defer akskresp.Body.Close()

	if err != nil {
		return nil, err
	}

	log.Info("RAJAT - Create AKSK Response - ", akskresp)
	bodyBytes, _ := ioutil.ReadAll(akskresp.Body)

	// Convert response body to string
	bodyString := string(bodyBytes)
	fmt.Println("AKSK API Response as String:\n" + bodyString)

	var backend = &model.GetAkSk{}
	json.Unmarshal(bodyBytes, &backend)
	fmt.Printf("Results: %v\n", backend)

	return backend, nil
}
