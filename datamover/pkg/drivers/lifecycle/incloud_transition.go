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

	"github.com/micro/go-micro/v2/client"
	"github.com/micro/go-micro/v2/metadata"
	log "github.com/sirupsen/logrus"

	"github.com/opensds/multi-cloud/api/pkg/common"
	migration "github.com/opensds/multi-cloud/datamover/pkg/drivers/https"
	. "github.com/opensds/multi-cloud/datamover/pkg/utils"
	datamover "github.com/opensds/multi-cloud/datamover/proto"
	"github.com/opensds/multi-cloud/s3/pkg/utils"
	osdss3 "github.com/opensds/multi-cloud/s3/proto"
)

func doInCloudTransition(acReq *datamover.LifecycleActionRequest) error {
	log.Infof("in-cloud transition action: transition %s from %d to %d of %s.\n",
		acReq.ObjKey, acReq.SourceTier, acReq.TargetTier, acReq.SourceBackend)

	log.Infof("in-cloud transition of object[%s], bucket:%v, target tier:%d\n", acReq.ObjKey, acReq.BucketName,
		acReq.TargetTier)
	req := &osdss3.MoveObjectRequest{
		SrcObject:        acReq.ObjKey,
		SrcObjectVersion: acReq.VersionId,
		SrcBucket:        acReq.BucketName,
		TargetTier:       acReq.TargetTier,
		MoveType:         utils.MoveType_ChangeStorageTier,
	}

	// add object to InProgressObjs
	if _, ok := InProgressObjs[acReq.ObjKey]; !ok {
		InProgressObjs[acReq.ObjKey] = struct{}{}
	} else {
		log.Warnf("the transition of object[%s] is in-progress\n", acReq.ObjKey)
		return errors.New(DMERR_TransitionInprogress)
	}

	// min is 1 minute, max is 30 days, default is 1 hour, user defined value should not less than min and not more than
	// max, otherwise default will be used
	tmout := migration.GetCtxTimeout("OBJECT_MOVE_TIME", SECONDS_ONE_MINUTE, SECONDS_30_DAYS, SECONDS_ONE_HOUR)
	ctx, _ := context.WithTimeout(context.Background(), tmout)
	ctx = metadata.NewContext(ctx, map[string]string{common.CTX_KEY_IS_ADMIN: strconv.FormatBool(true)})
	_, err := s3client.MoveObject(ctx, req, client.WithRequestTimeout(tmout))
	if err != nil {
		// if failed, it will try again in the next round schedule
		log.Errorf("in-cloud transition of %s failed:%v\n", acReq.ObjKey, err)
	} else {
		log.Infof("in-cloud transition of %s succeed.\n", acReq.ObjKey)
	}

	// remove object from InProgressObjs
	delete(InProgressObjs, acReq.ObjKey)

	return err
}
