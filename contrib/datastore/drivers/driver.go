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
	pb "github.com/opensds/multi-cloud/file/proto"
)

// define the storage driver interface.
type StorageDriver interface {
	FileStorageDriver
}

// define the fileshare driver interface.
type FileStorageDriver interface {
	CreateFileShare(ctx context.Context, fs *pb.CreateFileShareRequest) (*pb.CreateFileShareResponse, error)
	GetFileShare(ctx context.Context, fs *pb.GetFileShareRequest) (*pb.GetFileShareResponse, error)
	ListFileShare(ctx context.Context, fs *pb.ListFileShareRequest) (*pb.ListFileShareResponse, error)
	UpdatefileShare(ctx context.Context, fs *pb.UpdateFileShareRequest) (*pb.UpdateFileShareResponse, error)
	DeleteFileShare(ctx context.Context, in *pb.DeleteFileShareRequest) (*pb.DeleteFileShareResponse, error)
	// Close: cleanup when driver needs to be stopped.
	Close() error
}
