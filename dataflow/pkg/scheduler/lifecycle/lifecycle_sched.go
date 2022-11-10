// Copyright 2019 The soda Authors.
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
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/soda/multi-cloud/api/pkg/common"
	"github.com/soda/multi-cloud/api/pkg/utils/constants"
	"github.com/soda/multi-cloud/dataflow/pkg/db"
	"github.com/soda/multi-cloud/dataflow/pkg/kafka"
	. "github.com/soda/multi-cloud/dataflow/pkg/model"
	. "github.com/soda/multi-cloud/dataflow/pkg/utils"
	datamover "github.com/soda/multi-cloud/datamover/proto"
	s3error "github.com/soda/multi-cloud/s3/error"
	s3utils "github.com/soda/multi-cloud/s3/pkg/utils"
	osdss3 "github.com/soda/multi-cloud/s3/proto"
	s3 "github.com/soda/multi-cloud/s3/proto"

	"github.com/micro/go-micro/v2/client"
	"github.com/micro/go-micro/v2/metadata"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

const (
	MICRO_ENVIRONMENT = "MICRO_ENVIRONMENT"
	K8S               = "k8s"

	s3Service_Docker = "s3"
	s3Service_K8S    = "soda.multicloud.v1.s3"
)

var topicLifecycle = "lifecycle"

const TIME_LAYOUT_TIDB = "2006-01-02 15:04:05"

type InterRules []*InternalLifecycleRule

// map from a specific tier to an array of tiers, that means transition can happens from the specific tier to those tiers in the array.
var TransitionMap map[string]struct{}
var mutext sync.Mutex
var s3client = osdss3.NewS3Service(getEnv(), client.DefaultClient)

func getEnv() string {
	if os.Getenv(MICRO_ENVIRONMENT) == K8S {
		return s3Service_K8S
	}
	return s3Service_Docker
}

func loadStorageClassDefinition() error {
	res, _ := s3client.GetTierMap(context.Background(), &s3.BaseRequest{})
	if len(res.Transition) == 0 {
		log.Info("get transition map failed")
		return fmt.Errorf("get tier definition failed")
	} else {
		log.Infof("res.Transition:%v, res.Tier2Name:%+v", res.Transition, res.Tier2Name)
	}

	TransitionMap = make(map[string]struct{})
	for _, v := range res.Transition {
		TransitionMap[v] = struct{}{}
	}

	return nil
}

// Get liecycle rules for each bucket from db, and schedule according to those rules.
func ScheduleLifecycle() {
	log.Info("[ScheduleLifecycle] begin ...")
	// Load transition map.
	{
		mutext.Lock()
		defer mutext.Unlock()
		if len(TransitionMap) == 0 {
			err := loadStorageClassDefinition()
			if err != nil {
				log.Errorf("[ScheduleLifecycle]load storage classes failed: %v.\n", err)
				return
			}
		}
	}

	// List buckets with lifecycle configured.
	ctx := metadata.NewContext(context.Background(), map[string]string{common.CTX_KEY_IS_ADMIN: strconv.FormatBool(true)})
	listRsp, err := s3client.ListBucketLifecycle(ctx, &s3.BaseRequest{})
	if err != nil {
		log.Errorf("[ScheduleLifecycle]list buckets failed: %v.\n", err)
		return
	}

	for _, v := range listRsp.Buckets {
		// For each bucket, get the lifecycle rules, and schedule each rule.
		if v.LifecycleConfiguration == nil {
			log.Infof("[ScheduleLifecycle]bucket[%s] has no lifecycle rule.\n", v.Name)
			continue
		}

		log.Infof("[ScheduleLifecycle]bucket[%s] has lifecycle rule.\n", v.Name)

		err = handleBucketLifecyle(v.Name, v.LifecycleConfiguration)
		if err != nil {
			log.Errorf("[ScheduleLifecycle]handle bucket lifecycle for bucket[%s] failed, err:%v.\n", v.Name, err)
			continue
		}
	}

	log.Info("[ScheduleLifecycle] end ...")
}

// Need to lock the bucket, incase the schedule period is too short and the bucket is scheduled at the same time.
// Need to consider confliction between rules.
func handleBucketLifecyle(bucket string, rules []*osdss3.LifecycleRule) error {
	// Translate rules set by user to internal rules which can be sorted.

	// Lifecycle scheduling must be mutual excluded among several schedulers, so get lock first.
	ret := db.DbAdapter.LockBucketLifecycleSched(bucket)
	for i := 0; i < 3; i++ {
		if ret == LockSuccess {
			break
		} else if ret == LockBusy {
			return fmt.Errorf("lifecycle scheduling of bucket[%s] is in progress", bucket)
		} else {
			// Try to lock again, try three times at most
			ret = db.DbAdapter.LockBucketLifecycleSched(bucket)
		}
	}
	if ret != LockSuccess {
		log.Errorf("lock scheduling failed.\n")
		return fmt.Errorf("internal error: lock failed")
	}
	// Make sure unlock before return
	defer db.DbAdapter.UnlockBucketLifecycleSched(bucket)

	var inRules, abortRules InterRules
	for _, rule := range rules {
		if rule.Status == RuleStatusDisabled {
			continue
		}

		// actions
		for _, ac := range rule.Actions {
			var acType int
			if ac.Name == ActionNameExpiration {
				// Expiration
				acType = ActionExpiration
			} else if ac.Backend == "" {
				acType = ActionIncloudTransition
			} else {
				acType = ActionCrosscloudTransition
			}

			v := InternalLifecycleRule{Bucket: bucket, Days: ac.GetDays(), ActionType: acType, Tier: ac.GetTier(), Backend: ac.GetBackend(), TargetBucket: ac.GetTargetBucket()}
			if rule.GetFilter() != nil {
				v.Filter = InternalLifecycleFilter{Prefix: rule.Filter.Prefix}
			}
			inRules = append(inRules, &v)
		}

		if rule.AbortIncompleteMultipartUpload != nil && rule.AbortIncompleteMultipartUpload.DaysAfterInitiation > 0 {
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
		log.Debugf("action rule: %+v\n", *v)
	}
	// End: Log for debug
	schedSortedActionsRules(&inRules)

	sort.Stable(abortRules)
	// Begin: Log for debug
	for _, v := range abortRules {
		log.Debugf("abort rule: %+v\n", *v)
	}
	// End: Log for debug
	schedSortedAbortRules(&abortRules)

	return nil
}

func checkTransitionValidation(source int32, destination int32) bool {
	key := fmt.Sprintf("%d:%d", source, destination)
	if _, ok := TransitionMap[key]; !ok {
		return false
	}

	return true
}

func getObjects(r *InternalLifecycleRule, marker string, limit int32) ([]*osdss3.Object, error) {
	// Get objects by communicating with s3 service.
	filt := make(map[string]string)

	timeFilt := fmt.Sprintf("{\"lte\":\"%s\"}",
		time.Now().AddDate(0, 0, int(0-r.Days)).Format(TIME_LAYOUT_TIDB))
	filt[KLastModified] = timeFilt
	if r.ActionType != ActionExpiration {
		filt[KStorageTier] = strconv.Itoa(int(r.Tier))
	}

	log.Infof("The filter: %+v\n", filt)
	s3req := osdss3.ListObjectsRequest{
		Version:    constants.ListObjectsType2Int,
		Bucket:     r.Bucket,
		Filter:     filt,
		StartAfter: marker,
		MaxKeys:    limit,
	}
	if len(r.Filter.Prefix) > 0 {
		s3req.Prefix = r.Filter.Prefix
	}
	ctx := metadata.NewContext(context.Background(), map[string]string{common.CTX_KEY_IS_ADMIN: strconv.FormatBool(true)})
	log.Debugf("ListObjectsRequest:%+v\n", s3req)
	s3rsp, err := s3client.ListObjects(ctx, &s3req)
	if err != nil || s3rsp.GetErrorCode() != int32(s3error.ErrNoErr) {
		log.Errorf("list objects failed, req:%+v,  err:%v.\n", s3req, err)
		return nil, err
	}

	return s3rsp.Objects, nil
}

func schedSortedAbortRules(inRules *InterRules) {
	log.Debugln("schedSortedAbortRules begin ...")
	ctx := metadata.NewContext(context.Background(), map[string]string{common.CTX_KEY_IS_ADMIN: strconv.FormatBool(true)})
	for _, r := range *inRules {
		var uploadIdMarker = ""
		for {
			log.Debugf("list upload reqest, bucket:%s, UploadIdMarker:%s\n", r.Bucket, uploadIdMarker)
			req := osdss3.ListBucketUploadRequest{BucketName: r.Bucket, MaxUploads: 1000, UploadIdMarker: uploadIdMarker}
			s3rsp, err := s3client.ListBucketUploadRecords(ctx, &req)
			if err != nil {
				log.Errorf("schedule for rule[id=%s,bucket=%s] failed, err:%v\n", r.Id, r.Bucket, err)
				break
			}
			records := s3rsp.Result.Uploads
			if len(records) == 0 {
				log.Debugln("break because no upload record exist")
				break
			}
			for _, rc := range records {
				log.Debugf("abort upload part, ObjKey=%s, UploadId=%s\n", rc.Key, rc.UploadId)
				req := datamover.LifecycleActionRequest{
					ObjKey:     rc.Key,
					BucketName: r.Bucket,
					UploadId:   rc.UploadId,
					Action:     AbortIncompleteMultipartUpload,
				}
				// If send failed, then ignore it, because it will be re-sent in the next schedule round.
				sendActionRequest(&req)
			}
			if s3rsp.Result.IsTruncated == false {
				log.Debugln("break because result is truncated")
				break
			}
			uploadIdMarker = s3rsp.Result.NextUploadIdMarker
		}
	}
	log.Debugln("schedSortedAbortRules end ...")
}

func schedSortedActionsRules(inRules *InterRules) {
	log.Info("schedSortedActionsRules begin ...")
	dupCheck := map[string]struct{}{}
	for _, r := range *inRules {
		log.Debugf("rule: %v\n", r)
		var marker string
		var limit int32 = 1000
		for {
			objs, err := getObjects(r, marker, limit)
			if err != nil {
				break
			}
			log.Debugf("objects count: %d\n", len(objs))
			for _, obj := range objs {
				log.Debugf("obj: %v\n", obj)
				if obj.DeleteMarker == true {
					log.Infof("deleteMarker of object[%s] is set, no lifecycle action need.\n", obj.ObjectKey)
					continue
				}
				if r.ActionType != ActionExpiration && obj.Tier == s3utils.Tier999 {
					// archived object cannot be transit
					log.Infof("object[%s] is already archived.\n", obj.ObjectKey)
					continue
				}
				// Check if the object exist in the dupCheck map.
				if _, ok := dupCheck[obj.ObjectKey]; !ok {
					// Not exist means this object has not processed in this round of scheduling.
					if r.ActionType != ActionExpiration && obj.Tier == r.Tier &&
						(obj.Location == r.Backend || r.Backend == "") {
						// For transition, if target backend and storage class is the same as source backend and storage
						// class, then no transition is need.
						log.Infof("no need transition for object[%s], backend=%s, tier=%d\n", obj.ObjectKey, r.Backend, r.Tier)
						// in case different actions exist for an object at the same time, for example transition to aws after 30 days
						// and transition to azure after 30 days, we need to make sure only one action will be taken.
						dupCheck[obj.ObjectKey] = struct{}{}
						continue
					}

					//Send request.
					var action int32
					if r.ActionType == ActionExpiration {
						action = int32(ActionExpiration)
					} else if r.Backend == "" || obj.Location == r.Backend {
						r.Backend = obj.Location
						action = int32(ActionIncloudTransition)
					} else {
						action = int32(ActionCrosscloudTransition)
					}

					if r.ActionType != ActionExpiration && checkTransitionValidation(obj.Tier, r.Tier) != true {
						log.Infof("transition object[%s] from tier[%d] to tier[%d] is invalid.\n", obj.ObjectKey, obj.Tier, r.Tier)
						// in case different actions exist for an object at the same time, for example transition to aws after 30 days
						// and transition to azure after 30 days, we need to make sure only one action will be taken.
						dupCheck[obj.ObjectKey] = struct{}{}
						continue
					}
					log.Infof("lifecycle action: object=[%s] type=[%d] source-tier=[%d] target-tier=[%d] "+
						"source-backend=[%s] target-backend=[%s] and targetBucket=[%s].\n", obj.ObjectKey, r.ActionType, obj.Tier, r.Tier,
						obj.Location, r.Backend, r.TargetBucket)
					acreq := datamover.LifecycleActionRequest{
						ObjKey:        obj.ObjectKey,
						BucketName:    obj.BucketName,
						Action:        action,
						SourceTier:    obj.Tier,
						TargetTier:    r.Tier,
						SourceBackend: obj.Location,
						TargetBackend: r.Backend,
						TargetBucket:  r.TargetBucket,
						ObjSize:       obj.Size,
						VersionId:     obj.VersionId,
						StorageMeta:   obj.StorageMeta,
						ObjectId:      obj.ObjectId,
					}

					// If send failed, then ignore it, because it will be re-sent in the next schedule period.
					log.Debug("Schedule the lifecycle with request parameters:", acreq)
					sendActionRequest(&acreq)

					// Add object key to dupCheck so it will not be processed repeatedly in this round or scheduling.
					dupCheck[obj.ObjectKey] = struct{}{}
				} else {
					log.Infof("object[%s] is already handled in this schedule time.\n", obj.ObjectKey)
				}
				marker = obj.ObjectKey
			}
			if int32(len(objs)) < limit {
				break
			}
		}
	}
	log.Info("schedSortedActionsRules end ...")
}

func sendActionRequest(req *datamover.LifecycleActionRequest) error {
	log.Infof("Send lifecycle request to datamover: %v\n", req)
	data, err := json.Marshal(*req)
	if err != nil {
		log.Errorf("marshal run job request failed, err:%v\n", data)
		return err
	}

	return kafka.ProduceMsg(topicLifecycle, data)
}

func (r InterRules) Len() int {
	return len(r)
}

/*
 Less reports whether the element with index i should sort before the element with index j.
 Expiration action has higher priority than transition action.
 For the same action type, bigger Days has higher priority.
*/
func (r InterRules) Less(i, j int) bool {
	var ret bool
	if r[i].ActionType == ActionExpiration && r[i].ActionType < r[j].ActionType {
		ret = true
	} else if r[i].ActionType > r[j].ActionType && r[j].ActionType == ActionExpiration {
		ret = false
	} else {
		if r[i].Days >= r[j].Days {
			ret = true
		} else {
			ret = false
		}
	}

	return ret
}

func (r InterRules) Swap(i, j int) {
	tmp := r[i]
	r[i] = r[j]

	r[j] = tmp
}
