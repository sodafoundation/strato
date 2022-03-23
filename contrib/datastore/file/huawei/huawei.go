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

package huawei

import (
	"context"
	"fmt"

	"github.com/huaweicloud/golangsdk"

	"github.com/soda/multi-cloud/contrib/datastore/file/huawei/sfs"

	pb "github.com/soda/multi-cloud/file/proto"
)

type HwFSAdapter struct {
	client *golangsdk.ServiceClient
}

func (hd *HwFSAdapter) CreateFileShare(ctx context.Context, in *pb.CreateFileShareRequest) (*pb.CreateFileShareResponse, error) {

	hwsFsType := in.Fileshare.Metadata.Fields[HwSFSType].GetStringValue()

	if hwsFsType == HwSFS {
		sfsAdapter := &sfs.HwSfsAdapter{Client: hd.client}
		out, err := sfsAdapter.CreateFileShare(ctx, in)
		if err != nil {
			return nil, err
		}
		return out, nil
	}

	return nil, fmt.Errorf("no support for FS Type %s at Backend", HwSFSType)
}

func (hd *HwFSAdapter) GetFileShare(ctx context.Context, in *pb.GetFileShareRequest) (*pb.GetFileShareResponse, error) {

	hwsFsType := in.Fileshare.Metadata.Fields[HwSFSType].GetStringValue()

	if hwsFsType == HwSFS {
		sfsAdapter := &sfs.HwSfsAdapter{Client: hd.client}
		out, err := sfsAdapter.GetFileShare(ctx, in)
		if err != nil {
			return nil, err
		}
		return out, nil
	}

	return nil, fmt.Errorf("no support for FS Type %s at Backend", HwSFSType)
}

func (hd *HwFSAdapter) ListFileShare(ctx context.Context, in *pb.ListFileShareRequest) (*pb.ListFileShareResponse, error) {
	hwsFsType := in.Filter[HwSFSType]

	if hwsFsType == HwSFS {
		sfsAdapter := &sfs.HwSfsAdapter{Client: hd.client}
		out, err := sfsAdapter.ListFileShare(ctx, in)
		if err != nil {
			return nil, err
		}
		return out, nil
	}

	return nil, fmt.Errorf("no support for FS Type %s at Backend", HwSFSType)
}

func (hd *HwFSAdapter) UpdatefileShare(ctx context.Context, in *pb.UpdateFileShareRequest) (*pb.UpdateFileShareResponse, error) {
	hwsFsType := in.Fileshare.Metadata.Fields[HwSFSType].GetStringValue()

	if hwsFsType == HwSFS {
		sfsAdapter := &sfs.HwSfsAdapter{Client: hd.client}
		out, err := sfsAdapter.UpdatefileShare(ctx, in)
		if err != nil {
			return nil, err
		}
		return out, nil
	}

	return nil, fmt.Errorf("no support for FS Type %s at Backend", HwSFSType)
}

func (hd *HwFSAdapter) DeleteFileShare(ctx context.Context, in *pb.DeleteFileShareRequest) (*pb.DeleteFileShareResponse, error) {
	hwsFsType := in.Fileshare.Metadata.Fields[HwSFSType].GetStringValue()

	if hwsFsType == HwSFS {
		sfsAdapter := &sfs.HwSfsAdapter{Client: hd.client}
		out, err := sfsAdapter.DeleteFileShare(ctx, in)
		if err != nil {
			return nil, err
		}
		return out, nil
	}

	return nil, fmt.Errorf("no support for FS Type %s at Backend", HwSFSType)
}

func (hd *HwFSAdapter) Close() error {
	// TODO:
	return nil
}
