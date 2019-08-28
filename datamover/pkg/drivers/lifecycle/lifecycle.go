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
	"errors"
	"fmt"
	"sync"

	"github.com/micro/go-log"
	"github.com/micro/go-micro/client"
	backend "github.com/opensds/multi-cloud/backend/proto"
	flowtype "github.com/opensds/multi-cloud/dataflow/pkg/model"
	"github.com/opensds/multi-cloud/dataflow/pkg/utils"
	s3mover "github.com/opensds/multi-cloud/datamover/pkg/amazon/s3"
	blobmover "github.com/opensds/multi-cloud/datamover/pkg/azure/blob"
	cephs3mover "github.com/opensds/multi-cloud/datamover/pkg/ceph/s3"
	"github.com/opensds/multi-cloud/datamover/pkg/db"
	Gcps3mover "github.com/opensds/multi-cloud/datamover/pkg/gcp/s3"
	obsmover "github.com/opensds/multi-cloud/datamover/pkg/huawei/obs"
	ibmcosmover "github.com/opensds/multi-cloud/datamover/pkg/ibm/cos"
	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
	datamover "github.com/opensds/multi-cloud/datamover/proto"
	osdss3 "github.com/opensds/multi-cloud/s3/proto"
)

var bkendInfo map[string]*BackendInfo
var s3client osdss3.S3Service
var bkendclient backend.BackendService
var mutex sync.RWMutex

type Int2String map[int32]string

// map from cloud vendor name to it's map relation relationship between internal tier to it's storage class name.
var Int2ExtTierMap map[string]*Int2String

func Init() {
	log.Logf("Lifecycle datamover init.")
	s3client = osdss3.NewS3Service("s3", client.DefaultClient)
	bkendclient = backend.NewBackendService("backend", client.DefaultClient)
	bkendInfo = make(map[string]*BackendInfo)
}

func HandleMsg(msgData []byte) error {
	var acReq datamover.LifecycleActionRequest
	err := json.Unmarshal(msgData, &acReq)
	if err != nil {
		log.Logf("unmarshal lifecycle action request failed, err:%v\n", err)
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
	case utils.AbortIncompleteMultipartUpload:
		doAbortUpload(acReq)
	default:
		log.Logf("unsupported action type: %d.\n", acType)
	}
}

// force means get location information from database not from cache.
func getBackendInfo(backendName *string, force bool) (*BackendInfo, error) {
	if !force {
		loc, exist := bkendInfo[*backendName]
		if exist {
			return loc, nil
		}
	}

	if *backendName == "" {
		log.Log("get backend information failed, backendName is null.\n")
		return nil, errors.New(DMERR_InternalError)
	}

	bk, err := db.DbAdapter.GetBackendByName(*backendName)
	if err != nil {
		log.Logf("get backend[%s] information failed, err:%v\n", backendName, err)
		return nil, err
	} else {
		loca := &BackendInfo{bk.Type, bk.Region, bk.Endpoint, bk.BucketName,
			bk.Access, bk.Security, *backendName}
		log.Logf("refresh backend[name:%s, loca:%+v] successfully.\n", *backendName, *loca)
		bkendInfo[*backendName] = loca
		return loca, nil
	}
}

func deleteObjFromBackend(objKey string, loca *LocationInfo) error {
	if loca.VirBucket != "" {
		objKey = loca.VirBucket + "/" + objKey
	}
	var err error = nil
	switch loca.StorType {
	case flowtype.STOR_TYPE_AWS_S3:
		mover := s3mover.S3Mover{}
		err = mover.DeleteObj(objKey, loca)
	case flowtype.STOR_TYPE_IBM_COS:
		mover := ibmcosmover.IBMCOSMover{}
		err = mover.DeleteObj(objKey, loca)
	case flowtype.STOR_TYPE_HW_OBS, flowtype.STOR_TYPE_HW_FUSIONSTORAGE, flowtype.STOR_TYPE_HW_FUSIONCLOUD:
		mover := obsmover.ObsMover{}
		err = mover.DeleteObj(objKey, loca)
	case flowtype.STOR_TYPE_AZURE_BLOB:
		mover := blobmover.BlobMover{}
		err = mover.DeleteObj(objKey, loca)
	case flowtype.STOR_TYPE_CEPH_S3:
		mover := cephs3mover.CephS3Mover{}
		err = mover.DeleteObj(objKey, loca)
	case flowtype.STOR_TYPE_GCP_S3:
		mover := Gcps3mover.GcpS3Mover{}
		err = mover.DeleteObj(objKey, loca)
	default:
		err = fmt.Errorf("unspport storage type:%s", loca.StorType)
	}

	if err != nil {
		log.Logf("delete object[%s] from backend[type:%s,bucket:%s] failed.\n", objKey, loca.StorType, loca.BucketName)
	} else {
		log.Logf("delete object[%s] from backend[type:%s,bucket:%s] successfully.\n", objKey, loca.StorType, loca.BucketName)
	}

	return err
}
