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

package service

import (
	"context"
	"fmt"
	"github.com/opensds/multi-cloud/s3/pkg/utils"
	"os"
	"strconv"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/micro/go-micro/client"
	"github.com/opensds/multi-cloud/api/pkg/utils/obs"
	backend "github.com/opensds/multi-cloud/backend/proto"
	. "github.com/opensds/multi-cloud/s3/error"
	"github.com/opensds/multi-cloud/s3/pkg/db"
	"github.com/opensds/multi-cloud/s3/pkg/gc"
	"github.com/opensds/multi-cloud/s3/pkg/helper"
	"github.com/opensds/multi-cloud/s3/pkg/meta"
	"github.com/opensds/multi-cloud/s3/pkg/meta/util"
	. "github.com/opensds/multi-cloud/s3/pkg/utils"
	pb "github.com/opensds/multi-cloud/s3/proto"
	log "github.com/sirupsen/logrus"
)

type Int2String map[int32]string
type String2Int map[string]int32

// map from cloud vendor name to a map, which is used to map from internal tier to it's storage class name.
var Int2ExtTierMap map[string]*Int2String

// map from cloud vendor name to a map, which is used to map from storage class name to internal tier.
var Ext2IntTierMap map[string]*String2Int

// map from a specific tier to an array of tiers, that means transition can happens from the specific tier to those tiers in the array.
var TransitionMap map[int32][]int32
var SupportedClasses []pb.StorageClass

type s3Service struct {
	MetaStorage   *meta.Meta
	backendClient backend.BackendService
}

func NewS3Service() pb.S3Handler {
	host := os.Getenv("DB_HOST")
	dbstor := Database{Credential: "unkonwn", Driver: "tidb", Endpoint: host}
	db.Init(&dbstor)

	initStorageClass()
	cfg := meta.MetaConfig{
		CacheType: meta.CacheType(helper.CONFIG.MetaCacheType),
		TidbInfo:  helper.CONFIG.TidbInfo,
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	metaStor := meta.New(cfg)
	gc.Init(ctx, cancelFunc, metaStor)
	return &s3Service{
		MetaStorage:   metaStor,
		backendClient: backend.NewBackendService("backend", client.DefaultClient),
	}
}

func GetNameFromTier(tier int32, backendType string) (string, error) {
	v, ok := Int2ExtTierMap[backendType]
	if !ok {
		log.Errorf("get storage class of failed, no such backend type:%s.\n", backendType)
		return "", ErrInternalError
	}

	v2, ok := (*v)[tier]
	if !ok {
		log.Errorf("get storage class of tier[%d] failed, backendType=%s.\n", tier, backendType)
		return "", ErrInternalError
	}

	log.Infof("storage class of tier[%d] for backend type[%s] is %s.\n", tier, backendType, v2)
	return v2, nil
}

func loadAWSDefault(i2e *map[string]*Int2String, e2i *map[string]*String2Int) {
	t2n := make(Int2String)
	t2n[Tier1] = AWS_STANDARD
	t2n[Tier99] = AWS_STANDARD_IA
	t2n[Tier999] = AWS_GLACIER
	(*i2e)[OSTYPE_AWS] = &t2n

	n2t := make(String2Int)
	n2t[AWS_STANDARD] = Tier1
	n2t[AWS_STANDARD_IA] = Tier99
	n2t[AWS_GLACIER] = Tier999
	(*e2i)[OSTYPE_AWS] = &n2t
}

func loadOpenSDSDefault(i2e *map[string]*Int2String, e2i *map[string]*String2Int) {
	t2n := make(Int2String)
	t2n[Tier1] = AWS_STANDARD
	t2n[Tier99] = AWS_STANDARD_IA
	t2n[Tier999] = AWS_GLACIER
	(*i2e)[OSTYPE_OPENSDS] = &t2n

	n2t := make(String2Int)
	n2t[AWS_STANDARD] = Tier1
	n2t[AWS_STANDARD_IA] = Tier99
	n2t[AWS_GLACIER] = Tier999
	(*e2i)[OSTYPE_OPENSDS] = &n2t
}

func loadAzureDefault(i2e *map[string]*Int2String, e2i *map[string]*String2Int) {
	t2n := make(Int2String)
	t2n[Tier1] = string(azblob.AccessTierHot)
	t2n[Tier99] = string(azblob.AccessTierCool)
	t2n[Tier999] = string(azblob.AccessTierArchive)
	(*i2e)[OSTYPE_Azure] = &t2n

	n2t := make(String2Int)
	n2t[string(azblob.AccessTierHot)] = Tier1
	n2t[string(azblob.AccessTierCool)] = Tier99
	n2t[string(string(azblob.AccessTierArchive))] = Tier999
	(*e2i)[OSTYPE_Azure] = &n2t
}

func loadHWDefault(i2e *map[string]*Int2String, e2i *map[string]*String2Int) {
	t2n := make(Int2String)
	t2n[Tier1] = string(obs.StorageClassStandard)
	t2n[Tier99] = string(obs.StorageClassWarm)
	t2n[Tier999] = string(obs.StorageClassCold)
	(*i2e)[OSTYPE_OBS] = &t2n

	n2t := make(String2Int)
	n2t[string(obs.StorageClassStandard)] = Tier1
	n2t[string(obs.StorageClassWarm)] = Tier99
	n2t[string(obs.StorageClassCold)] = Tier999
	(*e2i)[OSTYPE_OBS] = &n2t
}

func loadGCPDefault(i2e *map[string]*Int2String, e2i *map[string]*String2Int) {
	t2n := make(Int2String)
	t2n[Tier1] = GCS_MULTI_REGIONAL
	//t2n[Tier99] = GCS_NEARLINE
	//t2n[Tier999] = GCS_COLDLINE
	(*i2e)[OSTYPE_GCS] = &t2n

	n2t := make(String2Int)
	n2t[GCS_MULTI_REGIONAL] = Tier1
	//n2t[GCS_NEARLINE] = Tier99
	//n2t[GCS_COLDLINE] = Tier999
	(*e2i)[OSTYPE_GCS] = &n2t
}

func loadCephDefault(i2e *map[string]*Int2String, e2i *map[string]*String2Int) {
	t2n := make(Int2String)
	t2n[Tier1] = CEPH_STANDARD
	(*i2e)[OSTYPE_CEPH] = &t2n

	n2t := make(String2Int)
	n2t[CEPH_STANDARD] = Tier1
	(*e2i)[OSTYPE_CEPH] = &n2t
}

func loadFusionStroageDefault(i2e *map[string]*Int2String, e2i *map[string]*String2Int) {
	t2n := make(Int2String)
	t2n[Tier1] = string(obs.StorageClassStandard)
	(*i2e)[OSTYPE_FUSIONSTORAGE] = &t2n

	n2t := make(String2Int)
	n2t[string(obs.StorageClassStandard)] = Tier1
	(*e2i)[OSTYPE_FUSIONSTORAGE] = &n2t
}

func loadDefaultStorageClass() error {
	/* Default storage class definition:
							T1		        T99					T999
	  AWS S3:				STANDARD		STANDARD_IA			GLACIER
	  Azure Blob:			HOT				COOL				ARCHIVE
	  HW OBS:				STANDARD		WARM				COLD
	  GCP:					Multi-Regional	NearLine			ColdLine
	  Ceph S3:				STANDARD		-					-
	  FusinoStorage Object: STANDARD		-					-
	*/
	/* Lifecycle transition:
		  T1 -> T99:  allowed
		  T1 -> T999: allowed
		  T99 -> T999: allowed
	      T99 -> T1:  not allowed
		  T999 -> T1: not allowed
		  T999 -> T99: not allowed
	*/

	SupportedClasses = append(SupportedClasses, pb.StorageClass{Name: string(AWS_STANDARD), Tier: int32(Tier1)})
	SupportedClasses = append(SupportedClasses, pb.StorageClass{Name: string(AWS_STANDARD_IA), Tier: int32(Tier99)})
	SupportedClasses = append(SupportedClasses, pb.StorageClass{Name: string(AWS_GLACIER), Tier: int32(Tier999)})

	log.Infof("Supported storage classes:%v\n", SupportedClasses)

	Int2ExtTierMap = make(map[string]*Int2String)
	Ext2IntTierMap = make(map[string]*String2Int)
	loadAWSDefault(&Int2ExtTierMap, &Ext2IntTierMap)
	loadOpenSDSDefault(&Int2ExtTierMap, &Ext2IntTierMap)
	loadAzureDefault(&Int2ExtTierMap, &Ext2IntTierMap)
	loadHWDefault(&Int2ExtTierMap, &Ext2IntTierMap)
	loadGCPDefault(&Int2ExtTierMap, &Ext2IntTierMap)
	loadCephDefault(&Int2ExtTierMap, &Ext2IntTierMap)
	loadFusionStroageDefault(&Int2ExtTierMap, &Ext2IntTierMap)

	log.Infof("Int2ExtTierMap:%v\n", Int2ExtTierMap)
	log.Infof("Ext2IntTierMap:%v\n", Ext2IntTierMap)

	return nil
}

// Currently user defined storage tiers and classes are not supported.
func loadUserDefinedStorageClass() error {
	log.Info("user defined storage class is not supported now")
	return fmt.Errorf("user defined storage class is not supported now")
}

func loadDefaultTransition() error {
	// transition from a tier to the same tier is valid in case cross-cloud transition
	TransitionMap = make(map[int32][]int32)
	TransitionMap[Tier1] = []int32{Tier1}
	TransitionMap[Tier99] = []int32{Tier1, Tier99}
	TransitionMap[Tier999] = []int32{Tier1, Tier99, Tier999}

	log.Infof("loadDefaultTransition:%+v\n", TransitionMap)
	return nil
}

func validTier(tier int32) bool {
	for _, v := range SupportedClasses {
		if v.Tier == tier {
			return true
		}
	}

	return false
}

func loadUserDefinedTransition() error {
	log.Info("user defined storage class is not supported now")
	return fmt.Errorf("user defined storage class is not supported now")
}

func initStorageClass() {
	// Check if use the default storage class.
	set := os.Getenv("USE_DEFAULT_STORAGE_CLASS")
	val, err := strconv.ParseInt(set, 10, 64)
	log.Infof("USE_DEFAULT_STORAGE_CLASS:set=%s, val=%d, err=%v.\n", set, val, err)
	if err != nil {
		log.Errorf("invalid USE_DEFAULT_STORAGE_CLASS:%s\n", set)
		panic("init s3service failed")
	}

	// Load storage class definition and transition relationship.
	var err1, err2 error
	if val > 0 {
		err1 = loadDefaultStorageClass()
		err2 = loadDefaultTransition()
	} else {
		err1 = loadUserDefinedStorageClass()
		err2 = loadUserDefinedTransition()
	}
	// Exit if init failed.
	if err1 != nil || err2 != nil {
		panic("init s3service failed")
	}
}

func (s *s3Service) GetStorageClasses(ctx context.Context, in *pb.BaseRequest, out *pb.GetStorageClassesResponse) error {
	classes := []*pb.StorageClass{}
	for _, v := range SupportedClasses {
		classes = append(classes, &pb.StorageClass{Name: v.Name, Tier: v.Tier})
	}

	out.Classes = classes

	return nil
}

func (s *s3Service) GetTierMap(ctx context.Context, in *pb.BaseRequest, out *pb.GetTierMapResponse) error {
	log.Info("GetTierMap ...")

	// Get map from internal tier to external class name.
	out.Tier2Name = make(map[string]*pb.Tier2ClassName)
	for k, v := range Int2ExtTierMap {
		var val pb.Tier2ClassName
		val.Lst = make(map[int32]string)
		for k1, v1 := range *v {
			val.Lst[k1] = v1
		}
		out.Tier2Name[k] = &val
	}

	// Get transition map.
	for k, v := range TransitionMap {
		for _, t := range v {
			trans := fmt.Sprintf("%d:%d", t, k)
			out.Transition = append(out.Transition, trans)
		}
	}

	log.Infof("out.Transition:%v\n", out.Transition)
	return nil
}

func (s *s3Service) UpdateBucket(ctx context.Context, in *pb.Bucket, out *pb.BaseResponse) error {
	log.Info("UpdateBucket is called in s3 service.")

	//TODO FIXME
	/*
		//update versioning if not nil
		if in.Versioning != nil {
			err := s.MetaStorage.Db.UpdateBucketVersioning(ctx, in.Name, in.Versioning.Status)
			if err != nil {
				log.Errorf("get bucket[%s] failed, err:%v\n", in.Name, err)
				return err
			}
		}

	*/
	if in.ServerSideEncryption != nil {
		byteArr, keyErr := utils.GetRandomNBitKey(32)
		if keyErr != nil {
			log.Error("Error generating SSE key", keyErr)
			return keyErr
		}
		ivArr, ivErr := utils.GetRandomNBitKey(16)
		if ivErr != nil {
			log.Error("Error generating SSE IV", ivErr)
			return ivErr
		}
		sseErr := s.MetaStorage.Db.UpdateBucketSSE(ctx, in.Name, in.ServerSideEncryption.SseType, byteArr, ivArr)
		if sseErr != nil {
			log.Error("Error creating SSE entry: ", sseErr)
			return sseErr
		}
	}
	return nil
}

func (s *s3Service) AppendObject(ctx context.Context, in *pb.BaseRequest, out *pb.BaseResponse) error {
	log.Info("UpdateBucket is called in s3 service.")

	return nil
}

func (s *s3Service) PostObject(ctx context.Context, in *pb.BaseRequest, out *pb.BaseResponse) error {
	log.Info("UpdateBucket is called in s3 service.")

	return nil
}

func (s *s3Service) HeadObject(ctx context.Context, in *pb.BaseObjRequest, out *pb.Object) error {
	log.Info("UpdateBucket is called in s3 service.")

	return nil
}

func (s *s3Service) GetObjACL(ctx context.Context, in *pb.BaseObjRequest, out *pb.ObjACL) error {
	log.Info("UpdateBucket is called in s3 service.")

	return nil
}

func (s *s3Service) GetBucketLocation(ctx context.Context, in *pb.BaseRequest, out *pb.BaseResponse) error {
	log.Info("UpdateBucket is called in s3 service.")

	return nil
}

func (s *s3Service) GetBucketVersioning(ctx context.Context, in *pb.BaseBucketRequest, out *pb.BucketVersioning) error {
	log.Info("UpdateBucket is called in s3 service.")

	return nil
}

//TODO Will check whether we need another interface for put bucket version
/*func (s *s3Service) PutBucketVersioning(ctx context.Context, in *pb.PutBucketVersioningRequest, out *pb.BaseResponse) error {
	log.Info("UpdateBucket is called in s3 service.")

	return nil
}

*/

func (s *s3Service) GetBucketACL(ctx context.Context, in *pb.BaseBucketRequest, out *pb.BucketACL) error {
	log.Info("UpdateBucket is called in s3 service.")

	return nil
}

func (s *s3Service) PutBucketCORS(ctx context.Context, in *pb.BaseRequest, out *pb.BaseResponse) error {
	log.Info("UpdateBucket is called in s3 service.")

	return nil
}

func (s *s3Service) GetBucketCORS(ctx context.Context, in *pb.BaseRequest, out *pb.BaseResponse) error {
	log.Info("UpdateBucket is called in s3 service.")

	return nil
}

func (s *s3Service) DeleteBucketCORS(ctx context.Context, in *pb.BaseRequest, out *pb.BaseResponse) error {
	log.Info("UpdateBucket is called in s3 service.")

	return nil
}

func (s *s3Service) PutBucketPolicy(ctx context.Context, in *pb.BaseRequest, out *pb.BaseResponse) error {
	log.Info("UpdateBucket is called in s3 service.")

	return nil
}

func (s *s3Service) GetBucketPolicy(ctx context.Context, in *pb.BaseRequest, out *pb.BaseResponse) error {
	log.Info("UpdateBucket is called in s3 service.")

	return nil
}

func (s *s3Service) DeleteBucketPolicy(ctx context.Context, in *pb.BaseRequest, out *pb.BaseResponse) error {
	log.Info("UpdateBucket is called in s3 service.")

	return nil
}

func (s *s3Service) HeadBucket(ctx context.Context, in *pb.BaseRequest, out *pb.Bucket) error {
	log.Info("UpdateBucket is called in s3 service.")

	return nil
}

func (s *s3Service) UpdateObjMeta(ctx context.Context, in *pb.UpdateObjMetaRequest, out *pb.BaseResponse) error {
	log.Infof("Update meatadata, objkey:%s, lastmodified:%d, setting:%v\n", in.ObjKey, in.LastModified, in.Setting)
	/*valid := make(map[string]struct{})
	valid["tier"] = struct{}{}
	valid["backend"] = struct{}{}
	set, err := CheckReqObjMeta(in.Setting, valid)
	if err.Code != ERR_OK {
		out.ErrorCode = fmt.Sprintf("%s", err.Code)
		out.Msg = err.Description
		return err.Error()
	}

	err = db.DbAdapter.UpdateObjMeta(ctx, &in.ObjKey, &in.BucketName, in.LastModified, set)
	if err.Code != ERR_OK {
		out.ErrorCode = fmt.Sprintf("%s", err.Code)
		out.Msg = err.Description
		return err.Error()
	}

	out.Msg = "update object meta data successfully."
	*/
	return nil
}

func (s *s3Service) GetBackendTypeByTier(ctx context.Context, in *pb.GetBackendTypeByTierRequest, out *pb.GetBackendTypeByTierResponse) error {
	for k, v := range Int2ExtTierMap {
		for k1, _ := range *v {
			if k1 == in.Tier {
				out.Types = append(out.Types, k)
			}
		}
	}

	log.Infof("GetBackendTypesByTier, types:%v\n", out.Types)

	return nil
}

func (s *s3Service) AddUploadRecord(ctx context.Context, record *pb.MultipartUploadRecord, out *pb.BaseResponse) error {
	log.Infof("add multipart upload record")

	return nil
}

func (s *s3Service) DeleteUploadRecord(ctx context.Context, record *pb.MultipartUploadRecord, out *pb.BaseResponse) error {
	log.Infof("delete multipart upload record")

	return nil
}

func (s *s3Service) CountObjects(ctx context.Context, in *pb.ListObjectsRequest, out *pb.CountObjectsResponse) error {
	log.Info("Count objects is called in s3 service.")

	rsp, err := s.MetaStorage.Db.CountObjects(ctx, in.Bucket, in.Prefix)
	if err != nil {
		return err
	}
	out.Count = rsp.Count
	out.Size = rsp.Size

	return nil
}

func GetErrCode(err error) (errCode int32) {
	if err == nil {
		errCode = int32(ErrNoErr)
		return
	}

	errCode = int32(ErrInternalError)
	s3err, ok := err.(S3ErrorCode)
	if ok {
		errCode = int32(s3err)
	}

	return errCode
}

func CheckRights(ctx context.Context, tenantId4Source string) (bool, string, string, error) {
	isAdmin, tenantId, userId, err := util.GetCredentialFromCtx(ctx)
	if err != nil {
		log.Errorf("get credential faied, err:%v\n", err)
		return isAdmin, tenantId, userId, ErrInternalError
	}
	if !isAdmin && tenantId != tenantId4Source {
		log.Errorf("access forbidden, tenantId=%s, tenantId4Source=%s\n", tenantId, tenantId4Source)
		return isAdmin, tenantId, userId, ErrAccessDenied
	}

	return isAdmin, tenantId, userId, nil
}
