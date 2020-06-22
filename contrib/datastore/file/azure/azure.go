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

package azure

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Azure/azure-storage-file-go/azfile"
	"github.com/opensds/multi-cloud/contrib/utils"

	backendpb "github.com/opensds/multi-cloud/backend/proto"
	fileUtils "github.com/opensds/multi-cloud/file/pkg/utils"
	file "github.com/opensds/multi-cloud/file/proto"
	log "github.com/sirupsen/logrus"
)

// TryTimeout indicates the maximum time allowed for any single try of an HTTP request.
var MaxTimeForSingleHttpRequest = 50 * time.Minute

type AzureAdapter struct {
	backend      *backendpb.BackendDetail
	pipeline      pipeline.Pipeline
}

func (ad *AzureAdapter) ParseFileShare(fs storage.Share) (*file.FileShare, error) {
	meta := fs.Metadata
	if meta == nil {
		meta = make(map[string]string)
	}

	meta[AZURE_ETAG] = fs.Properties.Etag
	meta[AZURE_LAST_MODIFIED] = fs.Properties.LastModified
	meta[AZURE_URL] = fs.URL()
	meta[AZURE_X_MS_SHARE_QUOTA] = strconv.FormatInt(int64(fs.Properties.Quota), 10)

	metadata, err := ConvertStructToStructMap(meta)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	fileshare := &file.FileShare{
		Name:                 fs.Name,
		Size:                 int64(fs.Properties.Quota) * utils.GB_FACTOR,
		Metadata:             metadata,
	}
	return fileshare, nil
}

func (ad *AzureAdapter) createPipeline(endpoint string, acountName string, accountKey string) (pipeline.Pipeline, error) {
	credential, err := azfile.NewSharedKeyCredential(acountName, accountKey)

	if err != nil {
		log.Infof("create credential[Azure File Share] failed, err:%v\n", err)
		return nil, err
	}

	//create Azure Pipeline
	p := azfile.NewPipeline(credential, azfile.PipelineOptions{
		Retry: azfile.RetryOptions{
			TryTimeout: MaxTimeForSingleHttpRequest,
		},
	})

	return p, nil
}

func (ad *AzureAdapter) createFileShareURL(fileshareName string) (azfile.ShareURL, error) {

	//create fileShareURL
	URL, _ := url.Parse(fmt.Sprintf("%s%s", ad.backend.Endpoint, fileshareName))
	//URL.Query().Set("timeout","20")

	return azfile.NewShareURL(*URL, ad.pipeline), nil
}

func (ad *AzureAdapter) GetFileShareProperties(ctx context.Context, fileshareName string) (*azfile.ShareGetPropertiesResponse, error) {

	shareURL, err := ad.createFileShareURL(fileshareName)
	if err != nil {
		log.Errorf("Create Azure File Share URL failed, err:%v\n", err)
		return nil, err
	}

	result, err := shareURL.GetProperties(ctx)
	if err != nil {
		log.Errorf("Get Azure File Share Properties failed, err:%v\n", err)
		return nil, err
	}
	return result, nil
}

func (ad *AzureAdapter) GetFileShareStatistics(ctx context.Context, fileshareName string) (*azfile.ShareStats, error) {

	shareURL, err := ad.createFileShareURL(fileshareName)
	if err != nil {
		log.Errorf("Create Azure File Share URL failed, err:%v\n", err)
		return nil, err
	}

	result, err := shareURL.GetStatistics(ctx)
	if err != nil {
		log.Errorf("Get Azure File Share Statistics failed, err:%v\n", err)
		return nil, err
	}
	return result, nil
}

func (ad *AzureAdapter) GetFileSharePermissions(ctx context.Context, fileshareName string) (*azfile.SignedIdentifiers, error) {

	shareURL, err := ad.createFileShareURL(fileshareName)
	if err != nil {
		log.Errorf("Create Azure File Share URL failed, err:%v\n", err)
		return nil, err
	}

	result, err := shareURL.GetPermissions(ctx)
	if err != nil {
		log.Errorf("Get Azure File Share Permissions failed, err:%v\n", err)
		return nil, err
	}
	return result, nil
}

func (ad *AzureAdapter) CreateFileShare(ctx context.Context, fs *file.CreateFileShareRequest) (*file.CreateFileShareResponse, error) {

	shareURL, err := ad.createFileShareURL(fs.Fileshare.Name)

	if err != nil {
		log.Infof("create Azure File Share URL failed, err:%v\n", err)
		return nil, err
	}

	metadata, err := ConvertStructToAzureMetadata(fs.Fileshare.Metadata)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	result, err := shareURL.Create(ctx, metadata, int32(fs.Fileshare.Size / utils.GB_FACTOR))
	if err != nil {
		log.Error(err)
		return nil, err
	}
	log.Infof("Create File share response = %+v", result.Response().Header)


	meta, err := ConvertHeaderToStruct(result.Response().Header)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	log.Infof("Create File share metadata = %+v", meta)

	return &file.CreateFileShareResponse{
		Fileshare: &file.FileShare{
			Name:                 fs.Fileshare.Name,
			Size:                 fs.Fileshare.Size,
			Status:               fileUtils.FileShareStateCreating,
			Metadata:             meta,
		},
	}, nil
}

func (ad *AzureAdapter) GetFileShare(ctx context.Context, fs *file.GetFileShareRequest) (*file.GetFileShareResponse, error) {

	getFS, err := ad.GetFileShareProperties(ctx, fs.Fileshare.Name)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	log.Infof("Get File share response = %+v", getFS.Response().Header)

	getFSstats, err := ad.GetFileShareStatistics(ctx, fs.Fileshare.Name)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	log.Infof("Get File share stats response = %+v", getFSstats.Response().Header)
	log.Infof("Get File share stats response Bytes = %+v", getFSstats.ShareUsageBytes)
/*
	getFSperm, err := ad.GetFileSharePermissions(ctx, fs.Fileshare.Name)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	log.Infof("Get File share permission response = %+v", getFSperm.Response().Header)
*/

	getFS.Response().Header.Set(AZURE_FILESHARE_USAGE_BYTES, strconv.FormatInt(int64(getFSstats.ShareUsageBytes), 10))
	meta, err := ConvertHeaderToStruct(getFS.Response().Header)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	log.Infof("Get File share metadata = %+v", meta)

	return &file.GetFileShareResponse{
		Fileshare: &file.FileShare{
			Name:                 fs.Fileshare.Name,
			Size:                 fs.Fileshare.Size,
			Status:               fileUtils.FileShareStateAvailable,
			Metadata:             meta,
		},
	}, nil
}

func (ad *AzureAdapter) ListFileShare(ctx context.Context, fs *file.ListFileShareRequest) (*file.ListFileShareResponse, error) {
	// List file share
	basicClient, err := storage.NewBasicClient(ad.backend.Access, ad.backend.Security)
	if err != nil {
		fmt.Println("Error in getting client")
		return nil, err
	}
	fsc := basicClient.GetFileService()
	result, err := fsc.ListShares(storage.ListSharesParameters{})
	if err != nil {
		fmt.Println("Error in response")
		return nil, err
	}
	log.Infof("List File share response = %+v", result)

	var fileshares []*file.FileShare
	for _, fileshare := range result.Shares {
		fs, err := ad.ParseFileShare(fileshare)
		if err != nil {
			log.Error(err)
			return nil, err
		}
		fs.Name = fileshare.Name
		fs.Status =  fileUtils.FileShareStateAvailable
		fileshares = append(fileshares, fs)
	}

	return &file.ListFileShareResponse{
		Fileshares: fileshares,
	}, nil
}

func (ad *AzureAdapter) UpdatefileShare(ctx context.Context, fs *file.UpdateFileShareRequest) (*file.UpdateFileShareResponse, error) {
	shareURL, err := ad.createFileShareURL(fs.Fileshare.Name)
	if err != nil {
		log.Infof("create Azure File Share URL failed, err:%v\n", err)
		return nil, err
	}

	result, err := shareURL.SetQuota(ctx, int32(fs.Fileshare.Size / utils.GB_FACTOR))
	if err != nil {
		log.Error(err)
		return nil, err
	}
	log.Infof("Update File share  Quota response = %+v", result.Response().Header)

	meta, err := ConvertHeaderToStruct(result.Response().Header)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	log.Infof("Update File share metadata = %+v", meta)

	fileshare := &file.FileShare{
		Name:                 fs.Fileshare.Name,
		Size:                 fs.Fileshare.Size,
		Status:               fileUtils.FileShareStateUpdating,
		Metadata: meta,
	}

	if fs.Fileshare.Metadata == nil {
		return &file.UpdateFileShareResponse{
			Fileshare: fileshare,
		}, nil
	}

	metadata, err := ConvertStructToAzureMetadata(fs.Fileshare.Metadata)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	metaRes, err := shareURL.SetMetadata(ctx, metadata)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	log.Infof("Update File share Metadata response = %+v", metaRes.Response().Header)

	meta, err = ConvertHeaderToStruct(metaRes.Response().Header)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	log.Infof("Update File share metadata = %+v for Azure backend", meta)

	fileshare.Metadata = meta

	return &file.UpdateFileShareResponse{
		Fileshare: fileshare,
	}, nil
}

func (ad *AzureAdapter) Close() error {
	// TODO:
	return nil
}
