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

package utils

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"github.com/micro/go-micro/v2/metadata"
	"github.com/opensds/multi-cloud/api/pkg/common"
	"github.com/opensds/multi-cloud/backend/proto"
	log "github.com/sirupsen/logrus"
)

type Database struct {
	Credential string `conf:"credential,username:password@tcp(ip:port)/dbname"`
	Driver     string `conf:"driver,mongodb"`
	Endpoint   string `conf:"endpoint,localhost:27017"`
}

// Tier1, Tier99 and Tier999 just like the tiers of hot, warm, cold.
// In the future, we will provide the ability for users to add new storage tiers, if we use 1, 2 and 3, then no space for new storage tiers.
const (
	NO_TIER = 0
	Tier1   = 1
	Tier99  = 99
	Tier999 = 999
)

const (
	AWS_STANDARD    = "STANDARD"
	AWS_STANDARD_IA = "STANDARD_IA"
	AWS_GLACIER     = "GLACIER"
)

const (
	CEPH_STANDARD = "STDANDARD"
)

const (
	GCS_STANDARD       = "STANDARD"
	GCS_MULTI_REGIONAL = "MULTI_REGIONAL"
	GCS_REGIONAL       = "REGIONAL"
	GCS_NEARLINE       = "NEARLINE"
	GCS_COLDLINE       = "COLDLINE"
	GCS_ARCHIVE        = "Archive"
)

const (
	ALIBABA_STANDARD = "Standard"
	ALIBABA_IA       = "IA"
	ALIBABA_ARCHIVE  = "Archive"
)

//Object Storage Type
const (
	OSTYPE_OPENSDS       = "OpenSDS"
	OSTYPE_AWS           = "aws-s3"
	OSTYPE_Azure         = "azure-blob"
	OSTYPE_OBS           = "hw-obs"
	OSTYPE_GCS           = "gcp-s3"
	OSTYPE_CEPH          = "ceph-s3"
	OSTYPE_FUSIONSTORAGE = "fusionstorage-object"
	OSTYPE_ALIBABA       = "alibaba-oss"
)

const (
	DBKEY_DELETEMARKER   = "isdeletemarker"
	DBKEY_INITFLAG       = "initflag"
	DBKEY_OBJECTKEY      = "objectkey"
	DBKEY_UPLOADID       = "uploadid"
	DBKEY_LASTMODIFIED   = "lastmodified"
	DBKEY_SUPPOSEDSTATUS = "supposedstatus"
	DBKEY_LOCKOBJ_OBJKEY = "objkey"
	DBKEY_BUCKET         = "bucket"
	DBKEY_INITTIME       = "inittime"
	DBKEY_NAME           = "name"
	DBKEY_LIFECYCLE      = "lifecycleconfiguration"
	DBKEY_ID             = "id"
)

type ObjsCountInfo struct {
	Size  int64
	Count int64
}

const (
	MaxObjectList  = 1000 // Limit number of objects in a listObjectsResponse.
	MaxUploadsList = 1000 // Limit number of uploads in a listUploadsResponse.
	MaxPartsList   = 1000 // Limit number of parts in a listPartsResponse.
)

const (
	VersioningEnabled   = "Enabled"
	VersioningDisabled  = "Disabled"
	VersioningSuspended = "Suspended"
)

type ListObjsAppendInfo struct {
	Prefixes   []string
	Truncated  bool
	NextMarker string
}

const (
	MoveType_Invalid = iota
	MoveType_MoveCrossBuckets
	MoveType_ChangeLocation
	MoveType_ChangeStorageTier
)

const (
	RequestType_Lifecycle = "lifecycle"
)

func Md5Content(data []byte) (base64Encoded, hexEncoded string) {
	md5ctx := md5.New()
	md5ctx.Write(data)
	cipherStr := md5ctx.Sum(nil)
	base64Encoded = base64.StdEncoding.EncodeToString(cipherStr)
	hexEncoded = hex.EncodeToString(cipherStr)
	return
}

func GetBackend(ctx context.Context, backedClient backend.BackendService, backendName string) (*backend.BackendDetail,
	error) {
	log.Infof("backendName is %v:\n", backendName)
	backendRep, backendErr := backedClient.ListBackend(ctx, &backend.ListBackendRequest{
		Offset: 0,
		Limit:  1,
		Filter: map[string]string{"name": backendName}})
	log.Infof("backendErr is %v:", backendErr)
	if backendErr != nil {
		log.Errorf("get backend %s failed.", backendName)
		return nil, backendErr
	}
	backend := backendRep.Backends[0]
	return backend, nil
}

func SetRepresentTenant(ctx context.Context, requestTenant, sourceTenant string) context.Context {
	if requestTenant != sourceTenant {
		md, _ := metadata.FromContext(ctx)
		md[common.CTX_REPRE_TENANT] = sourceTenant
		ctx = metadata.NewContext(ctx, md)
	}

	return ctx
}

const (
	REQUEST_HEADER_SSE_KEY          = "x-amz-server-side-encryption"
	REQUEST_HEADER_SSE_VALUE_AES256 = "AES256"
)
