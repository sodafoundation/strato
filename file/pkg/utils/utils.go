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

package utils

import (
	"context"

	"github.com/opensds/multi-cloud/backend/proto"
	log "github.com/sirupsen/logrus"
)

const (
	// It's RFC 8601 format that decodes and encodes with
	// exactly precision to seconds.
	TimeFormat = `2006-01-02T15:04:05`

	GB_FACTOR = 1024 * 1024 * 1024
)

const (
	// FileShareStateCreating is a FileShareState enum value
	FileShareStateCreating = "creating"

	// FileShareStateAvailable is a FileShareState enum value
	FileShareStateAvailable = "available"

	// FileShareStateInUse is a FileShareState enum value
	FileShareStateInUse = "inUse"

	// FileShareStateError is a FileShareState enum value
	FileShareStateError = "error"

	// FileShareStateUpdating is a FileShareState enum value
	FileShareStateUpdating = "updating"

	// FileShareStateDeleting is a FileShareState enum value
	FileShareStateDeleting = "deleting"

	// FileShareStateErrorDeleting is a FileShareState enum value
	FileShareStateErrorDeleting = "errorDeleting"

	// FileShareStateDeleted is a FileShareState enum value
	FileShareStateDeleted = "deleted"
)

func GetBackend(ctx context.Context, backendClient backend.BackendService,
	backendId string) (*backend.GetBackendResponse, error){
	backend, err := backendClient.GetBackend(ctx, &backend.GetBackendRequest{Id: backendId})
	if err != nil {
		log.Errorf("get backend %s failed.", backendId)
		return nil, err
	}
	log.Infof("backend response = [%+v]\n", backend)
	return backend, nil
}
