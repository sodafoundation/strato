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
	"encoding/json"
	"fmt"
	"sync"

	"github.com/opensds/multi-cloud/dataflow/pkg/kafka"
	datamover "github.com/opensds/multi-cloud/datamover/proto"

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

	/*

		var inRules, abortRules InterRules
		for _, rule := range rules {
			if rule.Status == RuleStatusDisabled {
				continue
			}

			// actions
			for _, ac := range rule.Actions {
				if ac.Backend == "" {
					// if no backend specified, then use the default backend of the bucket
					ac.Backend = defaultBackend
				}
				var acType int
				if ac.Name == ActionNameExpiration {
					// Expiration
					acType = ActionExpiration
				} else if ac.Backend == defaultBackend {
					acType = ActionIncloudTransition
				} else {
					acType = ActionCrosscloudTransition
				}

				v := InternalLifecycleRule{Bucket: bucket, Days: ac.GetDays(), ActionType: acType, Tier: ac.GetTier(), Backend: ac.GetBackend()}
				if rule.GetFilter() != nil {
					v.Filter = InternalLifecycleFilter{Prefix: rule.Filter.Prefix}
				}
				inRules = append(inRules, &v)
			}

			if rule.AbortIncompleteMultipartUpload.DaysAfterInitiation > 0 {
				// abort incomplete multipart uploads
				abortRule := InternalLifecycleRule{Bucket: bucket, Days: rule.AbortIncompleteMultipartUpload.DaysAfterInitiation, ActionType: AbortIncompleteMultipartUpload}
				if rule.GetFilter() != nil {
					abortRule.Filter = InternalLifecycleFilter{Prefix: rule.Filter.Prefix}
				}
				abortRules = append(abortRules, &abortRule)
			}
		}

		// Sort rules, in case different actions exist for an object at the same time, for example, expiration after 30 days
		// and transition to azure after 60 days, we need to make sure only one action will be taken, and that needs the
		// sorting be stable.
		sort.Stable(inRules)
		// Begin: Log for debug
		for _, v := range inRules {
			log.Logf("action rule: %+v\n", *v)
		}
		// End: Log for debug
		schedSortedActionsRules(&inRules)

		sort.Stable(abortRules)
		// Begin: Log for debug
		for _, v := range abortRules {
			log.Logf("abort rule: %+v\n", *v)
		}
		// End: Log for debug
		schedSortedAbortRules(&abortRules)

	*/

	return nil
}

/*
func schedSortedAbortRules(inRules *InterRules) {
	dupCheck := map[string]struct{}{}
	for _, r := range *inRules {
		var offset, limit int32 = 0, 1000
		for {
			req := osdss3.ListMultipartUploadRequest{Bucket: r.Bucket, Prefix: r.Filter.Prefix, Days: r.Days, Limit: limit, Offset: offset}
			s3rsp, err := s3client.ListUploadRecord(context.Background(), &req)
			if err != nil {
				log.Logf("schedule for rule[id=%s,bucket=%s] failed, err:%v\n", r.Id, r.Bucket, err)
				break
			}
			records := s3rsp.Records
			num := int32(len(records))
			offset += num
			log.Logf("schedSortedAbortRules:num=%d,offset=%d\n", num, offset)
			for _, rc := range records {
				if _, ok := dupCheck[rc.UploadId]; !ok {
					req := datamover.LifecycleActionRequest{
						ObjKey:        rc.ObjectKey,
						BucketName:    rc.Bucket,
						UploadId:      rc.UploadId,
						TargetBackend: rc.Backend,
						Action:        AbortIncompleteMultipartUpload,
					}
					// If send failed, then ignore it, because it will be re-sent in the next schedule period.
					sendActionRequest(&req)

					// Add object key to dupCheck so it will not be processed repeatedly in this round or scheduling.
					dupCheck[rc.UploadId] = struct{}{}

				} else {
					log.Logf("upload[id=%s] is already handled in this schedule time.\n", rc.UploadId)
				}
			}
			if num < limit {
				break
			}
		}
	}
}

func schedSortedActionsRules(inRules *InterRules) {
	dupCheck := map[string]struct{}{}
	for _, r := range *inRules {
		var offset, limit int32 = 0, 1000
		for {
			objs, err := getObjects(r, offset, limit)
			if err != nil {
				break
			}
			num := int32(len(objs))
			offset += num
			// Check if the object exist in the dupCheck map.
			for _, obj := range objs {
				if obj.IsDeleteMarker == "1" {
					log.Logf("deleteMarker of object[%s] is set, no lifecycle action need.\n", obj.ObjectKey)
					continue
				}
				if _, ok := dupCheck[obj.ObjectKey]; !ok {
					// Not exist means this object has is not processed in this round of scheduling.
					if r.ActionType != ActionExpiration && obj.Backend == r.Backend && obj.Tier == r.Tier {
						// For transition, if target backend and storage class is the same as source backend and storage class, then no transition is need.
						log.Logf("no need transition for object[%s], backend=%s, tier=%d\n", obj.ObjectKey, r.Backend, r.Tier)
						// in case different actions exist for an object at the same time, for example transition to aws after 30 days
						// and transition to azure after 30 days, we need to make sure only one action will be taken.
						dupCheck[obj.ObjectKey] = struct{}{}
						continue
					}

					//Send request.
					var action int32
					if r.ActionType == ActionExpiration {
						action = int32(ActionExpiration)
					} else if obj.Backend == r.Backend {
						action = int32(ActionIncloudTransition)
					} else {
						action = int32(ActionCrosscloudTransition)
					}

					if r.ActionType != ActionExpiration && checkTransitionValidation(obj.Tier, r.Tier) != true {
						log.Logf("transition object[%s] from tier[%d] to tier[%d] is invalid.\n", obj.ObjectKey, obj.Tier, r.Tier)
						// in case different actions exist for an object at the same time, for example transition to aws after 30 days
						// and transition to azure after 30 days, we need to make sure only one action will be taken.
						dupCheck[obj.ObjectKey] = struct{}{}
						continue
					}
					log.Logf("lifecycle action: object=[%s] type=[%d] source-tier=[%d] target-tier=[%d] source-backend=[%s] target-backend=[%s].\n",
						obj.ObjectKey, r.ActionType, obj.Tier, r.Tier, obj.Backend, r.Backend)
					acreq := datamover.LifecycleActionRequest{
						ObjKey:        obj.ObjectKey,
						BucketName:    obj.BucketName,
						Action:        action,
						SourceTier:    obj.Tier,
						TargetTier:    r.Tier,
						SourceBackend: obj.Backend,
						TargetBackend: r.Backend,
						ObjSize:       obj.Size,
						LastModified:  obj.LastModified,
					}

					// If send failed, then ignore it, because it will be re-sent in the next schedule period.
					sendActionRequest(&acreq)

					// Add object key to dupCheck so it will not be processed repeatedly in this round or scheduling.
					dupCheck[obj.ObjectKey] = struct{}{}
				} else {
					log.Logf("object[%s] is already handled in this schedule time.\n", obj.ObjectKey)
				}
			}
			if num < limit {
				break
			}
		}
	}
}

*/

func sendActionRequest(req *datamover.LifecycleActionRequest) error {
	log.Logf("Send lifecycle request to datamover: %v\n", req)
	data, err := json.Marshal(*req)
	if err != nil {
		log.Logf("marshal run job request failed, err:%v\n", data)
		return err
	}

	return kafka.ProduceMsg(topicLifecycle, data)
}
