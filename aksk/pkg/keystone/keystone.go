package keystone

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/opensds/multi-cloud/aksk/pkg/model"
	"github.com/opensds/multi-cloud/aksk/pkg/utils"
	log "github.com/sirupsen/logrus"
	"net/http"
)

const KEYSTONE_URL = "http://192.168.20.108/identity/v3/auth/"

type resp struct {
	credential map[string]string
	//map[string]map[string]string
}

func CreateAKSK( aksk *model.AkSk)(string, error) {
	accessKey, _ := utils.GenerateRandomString(16)
	secretKey, _ := utils.GenerateRandomString(32)
	blob := "{\"access\":\"" + accessKey + "\",\"secret\":\"" + secretKey +"\"}"
	log.Info("RAJAT - CreateAKSK - AKSK : ", blob)
	/*requestbody := &resp{
		credential:  map[string]string{
			"blob": blob,
			"project_id": aksk.ProjectId,
			"type": "ec2",
			"user_id": aksk.UserId,
		} ,
	}
	request, _ := json.Marshal(requestbody)
	*/
	mapD2 := map[string]string{"blob": string(blob),
		"project_id": "94b280022d0c4401bcf3b0ea85870519",
		"type": "ec2",
		"user_id": "558057c4256545bd8a307c37464003c9",}
	mapD3 := map[string]map[string]string{
		"credential" : mapD2,
	}
	mapB2, _ := json.Marshal(mapD2)
	fmt.Println(string(mapB2))
	mapB3, _ := json.Marshal(mapD3)
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

func DeleteAKSK() {

}

func GetAKSK() {

}
