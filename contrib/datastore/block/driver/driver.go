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

package driver

import (
	"context"
	pb "github.com/opensds/multi-cloud/block/proto"
)

// define the block driver interface.
type BlockDriver interface {
	CreateVolume(ctx context.Context, volume *pb.CreateVolumeRequest) (*pb.CreateVolumeResponse, error)
	GetVolume(ctx context.Context, volume *pb.GetVolumeRequest) (*pb.GetVolumeResponse, error)
	ListVolume(ctx context.Context, volume *pb.ListVolumeRequest) (*pb.ListVolumeResponse, error)
	UpdateVolume(ctx context.Context, volume *pb.UpdateVolumeRequest) (*pb.UpdateVolumeResponse, error)
	DeleteVolume(ctx context.Context, volume *pb.DeleteVolumeRequest) (*pb.DeleteVolumeResponse, error)
	// Close: cleanup when driver needs to be stopped.
	Close() error
}
