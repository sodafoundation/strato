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

package pkg

import (
	"context"
	"github.com/micro/go-log"
	"github.com/opensds/multi-cloud/s3/pkg/db"
	. "github.com/opensds/multi-cloud/s3/pkg/exception"
	pb "github.com/opensds/multi-cloud/s3/proto"
	"net/http"
	"os"
	"strconv"
	"fmt"
	. "github.com/opensds/multi-cloud/s3/pkg/utils"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/opensds/multi-cloud/api/pkg/utils/obs"
)

type Int2String map[int32]string
type String2Int map[string]int32
// map from cloud vendor name to it's map relation relationship between internal tier to it's storage class name.
var Int2ExtTierMap map[string]*Int2String
// map from cloud vendor name to it's map relation relationship between it's storage class name to internal tier.
var Ext2IntTierMap map[string]*String2Int
// map from a specific tier to an array of tiers, that means transition can happens from the specific tier to those tiers in the array.
var TransitionMap map[int32][]int32
var SupportedClasses []pb.StorageClass

type s3Service struct{}

func getTierFromName(className string) (int32, S3Error) {
	v, ok := Ext2IntTierMap[OSTYPE_OPENSDS]
	if !ok {
		log.Logf("Get tier of storage class[%s] failed.\n", className)
		return 0, InternalError
	}

	v2, ok := (*v)[className]
	if !ok {
		log.Logf("Get tier of storage class[%s] failed.\n", className)
		return 0, InternalError
	}

	log.Logf("Get tier of storage class[%s] successfully.\n", className)
	return v2, NoError
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
	t2n[Tier99] = GCS_NEARLINE
	t2n[Tier999] = GCS_COLDLINE
	(*i2e)[OSTYPE_GCS] = &t2n

	n2t := make(String2Int)
	n2t[GCS_MULTI_REGIONAL] = Tier1
	n2t[GCS_NEARLINE] = Tier99
	n2t[GCS_COLDLINE] = Tier999
	(*e2i)[OSTYPE_GCS] = &n2t
}

func loadCephDefault(i2e *map[string]*Int2String, e2i *map[string]*String2Int) {
	t2n := make(Int2String)
	t2n[Tier1] = CEPH_STANDARD
	(*i2e)[OSTYPE_CEPTH] = &t2n

	n2t := make(String2Int)
	n2t[CEPH_STANDARD] = Tier1
	(*e2i)[OSTYPE_OBS] = &n2t
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

	SupportedClasses = append(SupportedClasses, pb.StorageClass{Name:string(AWS_STANDARD), Tier:int32(Tier1)})
	SupportedClasses = append(SupportedClasses, pb.StorageClass{Name:string(AWS_STANDARD_IA), Tier:int32(Tier99)})
	SupportedClasses = append(SupportedClasses, pb.StorageClass{Name:string(AWS_GLACIER), Tier:int32(Tier999)})
	log.Logf("Supported storage classes:%v\n", SupportedClasses)

	Int2ExtTierMap = make(map[string]*Int2String)
	Ext2IntTierMap = make(map[string]*String2Int)
	loadAWSDefault(&Int2ExtTierMap, &Ext2IntTierMap)
	loadOpenSDSDefault(&Int2ExtTierMap, &Ext2IntTierMap)
	loadAzureDefault(&Int2ExtTierMap, &Ext2IntTierMap)
	loadHWDefault(&Int2ExtTierMap, &Ext2IntTierMap)
	loadGCPDefault(&Int2ExtTierMap, &Ext2IntTierMap)
	loadCephDefault(&Int2ExtTierMap, &Ext2IntTierMap)
	loadFusionStroageDefault(&Int2ExtTierMap, &Ext2IntTierMap)

	log.Logf("Int2ExtTierMap:%v\n", Int2ExtTierMap)
	log.Logf("Ext2IntTierMap:%v\n", Ext2IntTierMap)

	return nil
}

func loadUserDefinedStorageClass() error {
	log.Log("User defined storage class is not supported now.")
	return fmt.Errorf("User defined storage class is not supported now")
}

func loadDefaultTransition() error {
	TransitionMap = make(map[int32][]int32)
	TransitionMap[Tier99] = []int32{Tier1}
	TransitionMap[Tier999] = []int32{Tier1, Tier99}

	log.Logf("loadDefaultTransition:%+v\n", TransitionMap)
	return nil
}

func loadUserDefinedTransition() error  {
	log.Log("User defined storage class is not supported now.")
	return fmt.Errorf("User defined storage class is not supported now")
}

func initStorageClass() {
	// Check if use the default storage class.
	set := os.Getenv("USE_DEFAULT_STORAGE_CLASS")
	val, err := strconv.ParseInt(set, 10, 64)
	log.Logf("USE_DEFAULT_STORAGE_CLASS:set=%s, val=%d, err=%v.\n", set, val, err)
	if err != nil{
		log.Logf("Invalid USE_DEFAULT_STORAGE_CLASS:%s", set)
		panic("Init s3service failed")
	}

	// Load storage class definition and transition relationship.
	var err1, err2 error
	if val > 0 {
		err1 = loadDefaultStorageClass()
		err2 = loadDefaultTransition()
	} else {
		err1 = loadDefaultTransition()
		err2 = loadUserDefinedTransition()
	}
	// Exit if init failed.
	if err1 != nil || err2 != nil {
		panic("Init s3service failed")
	}
}

func (b *s3Service) GetStorageClasses(ctx context.Context, in *pb.BaseRequest, out *pb.GetStorageClassesResponse) error {
	classes := []*pb.StorageClass{}
	for _, v := range SupportedClasses {
		classes = append(classes, &pb.StorageClass{Name: v.Name, Tier: v.Tier})
	}

	out.Classes = classes

	return nil
}

func (b *s3Service) ListBuckets(ctx context.Context, in *pb.BaseRequest, out *pb.ListBucketsResponse) error {
	log.Log("ListBuckets is called in s3 service.")
	buckets := []pb.Bucket{}
	err := db.DbAdapter.ListBuckets(in, &buckets)
	if err.Code != ERR_OK {
		return err.Error()
	}
	for j := 0; j < len(buckets); j++ {
		if buckets[j].Deleted != true {
			out.Buckets = append(out.Buckets, &buckets[j])
		}
	}

	return nil
}

func (b *s3Service) CreateBucket(ctx context.Context, in *pb.Bucket, out *pb.BaseResponse) error {
	log.Log("CreateBucket is called in s3 service.")
	bucket := pb.Bucket{}
	err := db.DbAdapter.GetBucketByName(in.Name, &bucket)
	//err := db.DbAdapter.CreateBucket(in)

	if err.Code != ERR_OK && err.Code != http.StatusNotFound {
		return err.Error()
	}
	if err.Code == http.StatusNotFound {
		log.Log(".CreateBucket is called in s3 service.")
		err1 := db.DbAdapter.CreateBucket(in)
		if err1.Code != ERR_OK {
			return err.Error()
		}
	} else {
		log.Log(".UpdateBucket is called in s3 service.")
		in.Deleted = false
		err1 := db.DbAdapter.UpdateBucket(in)
		if err1.Code != ERR_OK {
			return err.Error()
		}
	}

	out.Msg = "Create bucket successfully."
	return nil
}

func (b *s3Service) GetBucket(ctx context.Context, in *pb.Bucket, out *pb.Bucket) error {
	log.Logf("GetBucket %s is called in s3 service.", in.Name)

	err := db.DbAdapter.GetBucketByName(in.Name, out)

	if err.Code != ERR_OK {
		return err.Error()
	}

	return nil
}

func (b *s3Service) DeleteBucket(ctx context.Context, in *pb.Bucket, out *pb.BaseResponse) error {
	log.Log("DeleteBucket is called in s3 service.")
	bucket := pb.Bucket{}
	err := db.DbAdapter.GetBucketByName(in.Name, &bucket)
	if err.Code != ERR_OK {
		return err.Error()
	}
	bucket.Deleted = true
	log.Log("UpdateBucket is called in s3 service.")
	err1 := db.DbAdapter.UpdateBucket(&bucket)
	if err1.Code != ERR_OK {
		return err.Error()
	}
	out.Msg = "Delete bucket successfully."

	return nil
}

func (b *s3Service) ListObjects(ctx context.Context, in *pb.ListObjectsRequest, out *pb.ListObjectResponse) error {
	log.Log("ListObject is called in s3 service.")
	objects := []pb.Object{}
	err := db.DbAdapter.ListObjects(in, &objects)

	if err.Code != ERR_OK {
		return err.Error()
	}
	for j := 0; j < len(objects); j++ {
		out.ListObjects = append(out.ListObjects, &objects[j])
	}

	return nil
}

func (b *s3Service) CreateObject(ctx context.Context, in *pb.Object, out *pb.BaseResponse) error {
	log.Log("PutObject is called in s3 service.")
	getObjectInput := pb.GetObjectInput{Bucket: in.BucketName, Key: in.ObjectKey}
	object := pb.Object{}
	err := db.DbAdapter.GetObject(&getObjectInput, &object)
	if err.Code != ERR_OK && err.Code != http.StatusNotFound {
		return err.Error()
	}
	if err.Code == http.StatusNotFound {
		log.Log("CreateObject is called in s3 service.")
		err1 := db.DbAdapter.CreateObject(in)
		if err1.Code != ERR_OK {
			return err.Error()
		}
	} else {
		log.Log("UpdateObject is called in s3 service.")
		err1 := db.DbAdapter.UpdateObject(in)
		if err1.Code != ERR_OK {
			return err.Error()
		}
	}
	out.Msg = "Create object successfully."

	return nil
}

func (b *s3Service) UpdateObject(ctx context.Context, in *pb.Object, out *pb.BaseResponse) error {
	log.Log("PutObject is called in s3 service.")
	err := db.DbAdapter.UpdateObject(in)
	if err.Code != ERR_OK {
		return err.Error()
	}
	out.Msg = "update object successfully."

	return nil
}

func (b *s3Service) GetObject(ctx context.Context, in *pb.GetObjectInput, out *pb.Object) error {
	log.Log("GetObject is called in s3 service.")
	err := db.DbAdapter.GetObject(in, out)
	if err.Code != ERR_OK {
		return err.Error()
	}
	return nil
}

func (b *s3Service) DeleteObject(ctx context.Context, in *pb.DeleteObjectInput, out *pb.BaseResponse) error {
	log.Log("DeleteObject is called in s3 service.")
	getObjectInput := pb.GetObjectInput{Bucket: in.Bucket, Key: in.Key}
	object := pb.Object{}
	err := db.DbAdapter.GetObject(&getObjectInput, &object)
	if err.Code != ERR_OK {
		return err.Error()
	}
	object.IsDeleteMarker = "1"
	log.Log("UpdateObject is called in s3 service.")
	err1 := db.DbAdapter.UpdateObject(&object)
	if err1.Code != ERR_OK {
		return err.Error()
	}
	out.Msg = "Delete object successfully."
	return nil
}

func NewS3Service() pb.S3Handler {
	host := os.Getenv("DB_HOST")
	dbstor := Database{Credential: "unkonwn", Driver: "mongodb", Endpoint: host}
	db.Init(&dbstor)

	initStorageClass()

	return &s3Service{}
}

func (b *s3Service)GetTierMap(ctx context.Context, in *pb.BaseRequest, out *pb.GetTierMapResponse) error {
	log.Log("GetTierMap ...")
	//out = &pb.GetTierMapResponse{}

	//Get map from internal tier to external class name.
	out.Tier2Name = make(map[string]*pb.Tier2ClassName)
	for k, v := range Int2ExtTierMap {
		var val pb.Tier2ClassName
		val.Lst = make(map[int32]string)
		for k1, v1 := range *v {
			val.Lst[k1] = v1
		}
		out.Tier2Name[k] = &val
	}

	//Get transition map.
	for k, v := range TransitionMap {
		for _, t := range v {
			trans := fmt.Sprintf("%d:%d", t, k)
			out.Transition = append(out.Transition, trans)
		}
	}

	log.Logf("out.Transition:%v\n", out.Transition)
	return nil
}

func (b *s3Service)UpdateObjMeta(ctx context.Context, in *pb.UpdateObjMetaRequest, out *pb.BaseResponse) error {
	log.Logf("Update meatadata, setting:%v\n", in.Setting)
	valid := make(map[string]struct{})
	valid["tier"] = struct{}{}
	valid["backend"] = struct{}{}
	ret, err := CheckReqObjMeta(in.Setting, valid)
	if err.Code != ERR_OK {
		out.ErrorCode = fmt.Sprintf("%s", err.Code)
		out.Msg = err.Description
		return err.Error()
	}

	err = db.DbAdapter.UpdateObjMeta(&in.ObjKey, &in.BucketName, ret)
	if err.Code != ERR_OK {
		out.ErrorCode = fmt.Sprintf("%s", err.Code)
		out.Msg = err.Description
		return err.Error()
	}

	out.Msg = "Update object meta data successfully."
	return nil
}

func CheckReqObjMeta(req map[string]string, valid map[string]struct{}) (map[string]interface{}, S3Error) {
	ret := make(map[string]interface{})
	for k, v := range req {
		if _, ok := valid[k]; !ok {
			log.Logf("CheckReqObjMeta: Invalid %s.\n", k)
			return nil, BadRequest
		}
		if k == "tier" {
			v1, err := strconv.Atoi(v)
			if err != nil {
				log.Logf("CheckReqObjMeta: Invalid tier:%s.\n", v)
				return nil, BadRequest
			}
			ret[k] = v1
		} else {
			ret[k] = v
		}
	}

	return ret, NoError
}

func (b *s3Service) GetBackendTypeByTier(ctx context.Context, in *pb.GetBackendTypeByTierRequest, out *pb.GetBackendTypeByTierResponse) error {
	for k, v := range Int2ExtTierMap {
		for k1, _ := range *v {
			if k1 == in.Tier {
				out.Types = append(out.Types, k)
			}
		}
	}

	log.Logf("GetBackendTypesByTier, types:%v\n", out.Types)

	return nil
}