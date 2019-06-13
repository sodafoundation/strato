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

package restoreobject

import (
	"fmt"
	"sync"

	"github.com/micro/go-log"
	"github.com/micro/go-micro/client"
	"github.com/opensds/multi-cloud/dataflow/pkg/db"
	. "github.com/opensds/multi-cloud/dataflow/pkg/model"
	osdss3 "github.com/opensds/multi-cloud/s3/proto"
	s3 "github.com/opensds/multi-cloud/s3/proto"
	"golang.org/x/net/context"
)

var topicLifecycle = "restoreobject"
var s3client = osdss3.NewS3Service("s3", client.DefaultClient)

var mutext sync.Mutex

// Get restore marker from each object from db, and schedule HEAD Object according to those values.
func ScheduleRestoreObject() {
	log.Log("[ScheduleRestoreObject] begin ...")

	// Get bucket list.
	listReq := s3.BaseRequest{Id: "test"}
	listBucketRsp, err := s3client.ListBuckets(context.Background(), &listReq)
	if err != nil {
		log.Logf("[ScheduleRestoreObject]list buckets failed: %v.\n", err)
		return
	}

	for _, v := range listBucketRsp.Buckets {
		//For each bucket, list all the objects
		getObjectInput := s3.ListObjectsRequest{Bucket: v.Name}
		listObjectRsp, err := s3client.ListObjects(context.Background(), &getObjectInput)
		if err != nil {
			log.Logf("[ScheduleRestoreObject]list objects failed: %v.\n", err)
			return
		}
		// For each object, get the restore marker rules, and schedule HEAD API
		for _, object := range listObjectRsp.ListObjects {
			restoreStatus := object.RestoreStatus
			if restoreStatus == nil {
				log.Logf("[ScheduleRestoreObject]object[%s] is not in restoration process.\n", object.ObjectKey)
				continue
			}
			log.Logf("[ScheduleRestoreObject]object[%s] is in restoration process\n", object.ObjectKey)

			err := handleRestoreObject(object.ObjectKey)
			if err != nil {
				log.Logf("[ScheduleRestoreObject]handle restore for object[%s] failed, err:%v.\n", object.ObjectKey, err)
				continue
			}
		}

		log.Log("[ScheduleRestoreObject] end ...")
	}
}

// Need to lock the bucket, in case the schedule period is too short and the object is scheduled at the same time.

func handleRestoreObject(objectKey string) error {

	// restore object must be mutual exclusive among several schedulers, so get lock first.
	ret := db.DbAdapter.LockRestoreObjectSched(objectKey)
	for i := 0; i < 3; i++ {
		if ret == LockSuccess {
			break
		} else if ret == LockBusy {
			return fmt.Errorf("restore scheduling of object[%s] is in progress", objectKey)
		} else {
			// Try to lock again, try three times at most
			ret = db.DbAdapter.LockRestoreObjectSched(objectKey)
		}
	}
	if ret != LockSuccess {
		log.Logf("lock scheduling failed.\n")
		return fmt.Errorf("internal error: lock failed")
	}
	// Make sure unlock before return
	defer db.DbAdapter.UnlockRestoreObjectSched(objectKey)

	schedRestoreObject(objectKey)

	return nil
}

func schedRestoreObject(objectKey string) {
	return
}
