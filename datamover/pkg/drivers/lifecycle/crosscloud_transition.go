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
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/micro/go-micro/metadata"
	"github.com/opensds/multi-cloud/api/pkg/common"
	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
	"github.com/opensds/multi-cloud/datamover/proto"
	"github.com/opensds/multi-cloud/s3/pkg/utils"
	osdss3 "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

// If transition for an object is in-progress, then the next transition message will be abandoned.
var InProgressObjs = make(map[string]struct{})

func copyObj(obj *osdss3.Object, targetLoc *LocationInfo) error {
	log.Infof("copy object[%s], size=%d\n", obj.ObjectKey, obj.Size)

	// add object to InProgressObjs
	if _, ok := InProgressObjs[obj.ObjectKey]; !ok {
		InProgressObjs[obj.ObjectKey] = struct{}{}
	} else {
		log.Infof("the transition of object[%s] is in-progress\n", obj.ObjectKey)
		return errors.New(DMERR_TransitionInprogress)
	}

	// copy object
	ctx, _ := context.WithTimeout(context.Background(), CLOUD_OPR_TIMEOUT*time.Second)
	ctx = metadata.NewContext(ctx, map[string]string{common.CTX_KEY_IS_ADMIN: strconv.FormatBool(true)})
	req := &osdss3.MoveObjectRequest{
		SrcObject:      obj.ObjectKey,
		SrcBucket:      obj.BucketName,
		TargetLocation: targetLoc.BakendName,
		TargetTier:     targetLoc.Tier,
		MoveType:       utils.MoveType_ChangeLocation,
		SourceType:     utils.MoveSourceType_Lifecycle,
	}

	_, err := s3client.MoveObject(ctx, req)
	if err != nil {
		// if failed, it will try again in the next round schedule
		log.Errorf("copy object[%s] failed, err:%\v", obj.ObjectKey, err)
	} else {
		log.Infof("copy object[%s] succeed\v", obj.ObjectKey)
	}

	// remove object from InProgressObjs
	delete(InProgressObjs, obj.ObjectKey)

	return err
}

func doCrossCloudTransition(acReq *datamover.LifecycleActionRequest) error {
	log.Infof("cross-cloud transition action: transition %s from %d of %s to %d of %s.\n",
		acReq.ObjKey, acReq.SourceTier, acReq.SourceBackend, acReq.TargetTier, acReq.TargetBackend)

	src := &LocationInfo{BucketName: acReq.BucketName, BakendName: acReq.SourceBackend}
	target := &LocationInfo{BucketName: acReq.BucketName, BakendName: acReq.TargetBackend, Tier: acReq.TargetTier}

	log.Infof("transition object[%s] from [%+v] to [%+v]\n", acReq.ObjKey, src, target)
	obj := osdss3.Object{ObjectKey: acReq.ObjKey, Size: acReq.ObjSize, BucketName: acReq.BucketName,
		Tier: acReq.SourceTier}
	err := copyObj(&obj, target)
	if err != nil {
		log.Errorf("cross-cloud transition of %s failed:%v\n", acReq.ObjKey, err)
	} else {
		log.Infof("cross-cloud transition of %s succeed.\n", acReq.ObjKey)
	}

	return err
}
