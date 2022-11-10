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

	block "github.com/soda/multi-cloud/block/proto"
)

// define the block driver interface.
type BlockDriver interface {
	CreateVolume(ctx context.Context, volume *block.CreateVolumeRequest) (*block.CreateVolumeResponse, error)
	GetVolume(ctx context.Context, volume *block.GetVolumeRequest) (*block.GetVolumeResponse, error)
	ListVolume(ctx context.Context, volume *block.ListVolumeRequest) (*block.ListVolumeResponse, error)
	UpdateVolume(ctx context.Context, volume *block.UpdateVolumeRequest) (*block.UpdateVolumeResponse, error)
	DeleteVolume(ctx context.Context, volume *block.DeleteVolumeRequest) (*block.DeleteVolumeResponse, error)
	// Close: cleanup when driver needs to be stopped.
	Close() error
}
