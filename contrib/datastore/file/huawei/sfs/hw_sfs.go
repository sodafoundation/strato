// Copyright 2020 The SODA Authors.
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

package sfs

import (
	"context"
	"fmt"

	"github.com/huaweicloud/golangsdk"
	"github.com/huaweicloud/golangsdk/openstack/sfs/v2/shares"
	"github.com/micro/go-micro/v2/util/log"

	"github.com/opensds/multi-cloud/contrib/utils"

	pb "github.com/opensds/multi-cloud/file/proto"
)

type HwSfsAdapter struct {
	Client *golangsdk.ServiceClient
}

func (hd *HwSfsAdapter) ParseFileShare(share *shares.Share) (*pb.FileShare, error) {
	meta := map[string]interface{}{
		ShareID:        share.ID,
		VolumeType:     share.VolumeType,
		CreatedAt:      share.CreatedAt,
		IsPublic:       share.IsPublic,
		Links:          share.Links,
		ProjectID:      share.ProjectID,
		ShareType:      share.ShareType,
		ExportLocation: share.ExportLocation,
		Host:           share.Host,
		ShareNetworkID: share.ShareNetworkID,
		SnapshotID:     share.SnapshotID,
		HwSFSType:      HwSFS,
	}

	if len(share.ExportLocations) > 0 {
		meta[ExportLocations] = share.ExportLocations
	}

	metadata, err := utils.ConvertMapToStruct(meta)
	if err != nil {
		log.Errorf("failed to convert metadata = [%+v] to struct", metadata, err)
		return nil, err
	}

	fileshare := &pb.FileShare{
		Name:     share.Name,
		Size:     int64(share.Size * utils.GB_FACTOR),
		Status:   share.Status,
		Metadata: metadata,
	}

	return fileshare, nil
}

func (hd *HwSfsAdapter) CreateFileShare(ctx context.Context, in *pb.CreateFileShareRequest) (*pb.CreateFileShareResponse, error) {

	metadataHW := make(map[string]string)
	for _, tag := range in.Fileshare.Tags {
		metadataHW[tag.Key] = tag.Value
	}

	if in.Fileshare.Encrypted {
		metadataHW[SfsCryptKeyId] = in.Fileshare.EncryptionSettings[KmsKeyId]
		metadataHW[SfsCryptAlias] = in.Fileshare.EncryptionSettings[KmsKeyName]
		metadataHW[SfsCryptDomainId] = in.Fileshare.EncryptionSettings[DomainId]
	}

	options := &shares.CreateOpts{
		Name:             in.Fileshare.Name,
		Description:      in.Fileshare.Description,
		Size:             int(in.Fileshare.Size / utils.GB_FACTOR),
		ShareProto:       in.Fileshare.Protocols[0],
		AvailabilityZone: in.Fileshare.AvailabilityZone,
		Metadata:         metadataHW,
	}

	share, err := shares.Create(hd.Client, options).Extract()
	if err != nil {
		return nil, fmt.Errorf("create Fileshare operation failed: %v", err)
	}
	log.Debugf("Create File share response = %+v", share)

	log.Infof("Create File share, Operation Resource: %s submitted to cloud backend", share.Name)

	fileshare, err := hd.ParseFileShare(share)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	if in.Fileshare.Encrypted {
		fileshare.Encrypted = in.Fileshare.Encrypted
		fileshare.EncryptionSettings = in.Fileshare.EncryptionSettings
	}

	go hd.UpdateFileShareVPC(hd.Client, share, in.Fileshare)

	return &pb.CreateFileShareResponse{
		Fileshare: fileshare,
	}, nil
}

func (hd *HwSfsAdapter) GetFileShare(ctx context.Context, in *pb.GetFileShareRequest) (*pb.GetFileShareResponse, error) {
	shareID := in.Fileshare.Metadata.Fields[ShareID].GetStringValue()
	share, err := hd.getShare(hd.Client, shareID)
	if err != nil {
		return nil, err
	}
	log.Debugf("Get File share response = %+v", share)

	fileshare, err := hd.ParseFileShare(share)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	if in.Fileshare.Encrypted {
		fileshare.Encrypted = in.Fileshare.Encrypted
		fileshare.EncryptionSettings = in.Fileshare.EncryptionSettings
	}

	return &pb.GetFileShareResponse{
		Fileshare: fileshare,
	}, nil
}

func (hd *HwSfsAdapter) ListFileShare(ctx context.Context, in *pb.ListFileShareRequest) (*pb.ListFileShareResponse, error) {
	shares, err := shares.List(hd.Client, shares.ListOpts{})
	if err != nil {
		log.Errorf("failed to List File shares: %v", err)
	}

	var fileshares []*pb.FileShare
	for _, share := range shares {
		fs, err := hd.ParseFileShare(&share)
		if err != nil {
			log.Error(err)
			return nil, err
		}
		fileshares = append(fileshares, fs)
	}

	log.Debugf("List File shares = %+v", fileshares)

	return &pb.ListFileShareResponse{
		Fileshares: fileshares,
	}, nil
}

func (hd *HwSfsAdapter) UpdatefileShare(ctx context.Context, in *pb.UpdateFileShareRequest) (*pb.UpdateFileShareResponse, error) {

	var share *shares.Share
	var err error
	shareID := in.Fileshare.Metadata.Fields[ShareID].GetStringValue()
	if len(in.Fileshare.Description) > 0 {
		options := &shares.UpdateOpts{DisplayName: in.Fileshare.Name, DisplayDescription: in.Fileshare.Description}
		share, err = shares.Update(hd.Client, shareID, options).Extract()
		if err != nil {
			return nil, err
		}
		log.Debugf("Update File share response = %+v", share)
	} else {
		share, err = hd.getShare(hd.Client, shareID)
		if err != nil {
			return nil, err
		}
	}

	fileshare, err := hd.ParseFileShare(share)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	expandOpts := shares.ExpandOpts{OSExtend: shares.OSExtendOpts{NewSize: int(in.Fileshare.Size / utils.GB_FACTOR)}}

	expand := shares.Expand(hd.Client, shareID, expandOpts)
	if expand.Err != nil {
		log.Error(expand.Err)
		return nil, expand.Err
	}
	log.Infof("Resize File share, Operation Resource: %s submitted to cloud backend", share.Name)

	return &pb.UpdateFileShareResponse{
		Fileshare: fileshare,
	}, nil
}

func (hd *HwSfsAdapter) DeleteFileShare(ctx context.Context, in *pb.DeleteFileShareRequest) (*pb.DeleteFileShareResponse, error) {
	shareID := in.Fileshare.Metadata.Fields[ShareID].GetStringValue()
	result := shares.Delete(hd.Client, shareID)
	if _, ok := result.Err.(golangsdk.ErrDefault404); ok {
		log.Infof("share %s not found, assuming it to be already deleted", shareID)
	} else {
		return nil, result.Err
	}

	log.Debugf("Delete File share response = %+v", result)

	return &pb.DeleteFileShareResponse{}, nil
}

func (hd *HwSfsAdapter) UpdateFileShareVPC(client *golangsdk.ServiceClient, share *shares.Share, fs *pb.FileShare) {

	err := hd.checkForStatus(client, share.ID, AvailableState, 3)
	if err != nil {
		log.Errorf("failed to get available status for share: %v", err)
		return
	}

	if err := hd.grantAccess(client, share.ID, fs.Metadata.Fields[VpcID].GetStringValue()); err != nil {
		log.Errorf("failed to grant access for share: %v", share.ID, err)
		return
	}
}

func (hd *HwSfsAdapter) checkForStatus(client *golangsdk.ServiceClient, shareID string, desiredStatus string, timeout int) error {
	return golangsdk.WaitFor(timeout, func() (bool, error) {
		share, err := hd.getShare(client, shareID)
		if err != nil {
			return false, err
		}
		return share.Status == desiredStatus, nil
	})
}

func (hd *HwSfsAdapter) getShare(client *golangsdk.ServiceClient, shareID string) (*shares.Share, error) {
	return shares.Get(client, shareID).Extract()
}

func (hd *HwSfsAdapter) grantAccess(client *golangsdk.ServiceClient, shareID string, vpcID string) error {

	grantAccessOpts := shares.GrantAccessOpts{
		AccessLevel: RWAccessLevel,
		AccessType:  CertAccessType,
		AccessTo:    vpcID,
	}

	access, err := shares.GrantAccess(client, shareID, grantAccessOpts).ExtractAccess()
	if err != nil {
		return err
	}
	log.Debugf("Granted access [%+v] for share: %s", access, shareID)

	return nil
}
