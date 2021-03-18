// Copyright 2019 The OpenSDS Authors.
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

package lifecycle

import (
	"encoding/json"
	"sync"
	"os"
	"github.com/micro/go-micro/v2/client"
	backend "github.com/opensds/multi-cloud/backend/proto"
	"github.com/opensds/multi-cloud/dataflow/pkg/utils"
	datamover "github.com/opensds/multi-cloud/datamover/proto"
	osdss3 "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

var s3client osdss3.S3Service
var bkendclient backend.BackendService
var mutex sync.RWMutex

const SECONDS_ONE_MINUTE = 60
const SECONDS_30_DAYS = 2592000
const SECONDS_ONE_HOUR = 3600

type Int2String map[int32]string

// map from cloud vendor name to it's map relation relationship between internal tier to it's storage class name.
var Int2ExtTierMap map[string]*Int2String

func Init() {
	log.Infof("Lifecycle datamover init.")
	s3Service := "s3"
	backendService := "backend"

	if(os.Getenv("MICRO_ENVIRONMENT") == "k8s"){
		s3Service = "soda.multicloud.v1.s3"
		backendService = "soda.multicloud.v1.backend"
	}

	s3client = osdss3.NewS3Service(s3Service, client.DefaultClient)
	bkendclient = backend.NewBackendService(backendService, client.DefaultClient)
}

func HandleMsg(msgData []byte) error {
	var acReq datamover.LifecycleActionRequest
	err := json.Unmarshal(msgData, &acReq)
	if err != nil {
		log.Errorf("unmarshal lifecycle action request failed, err:%v\n", err)
		return err
	}

	go doAction(&acReq)

	return nil
}

func doAction(acReq *datamover.LifecycleActionRequest) {
	acType := int(acReq.Action)
	switch acType {
	case utils.ActionCrosscloudTransition:
		doCrossCloudTransition(acReq)
	case utils.ActionIncloudTransition:
		doInCloudTransition(acReq)
	case utils.ActionExpiration:
		doExpirationAction(acReq)
	// This is left until multipart upload is implemented.
	case utils.AbortIncompleteMultipartUpload:
		doAbortUpload(acReq)
	default:
		log.Infof("unsupported action type: %d.\n", acType)
	}
}
