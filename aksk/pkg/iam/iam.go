// Copyright 2021 The SODA Authors.
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

package iam

import (
	"context"

	log "github.com/sirupsen/logrus"

	"github.com/opensds/multi-cloud/aksk/pkg/iam/driver/keystone"
	"github.com/opensds/multi-cloud/aksk/pkg/model"
	"github.com/opensds/multi-cloud/aksk/pkg/utils/config"
	pb "github.com/opensds/multi-cloud/aksk/proto"
)

type IAMAuthenticator interface {
	// AkSk
	CreateAkSk(aksk *model.AkSk, req *pb.AkSkCreateRequest) (*model.AkSkOut, error)
	DeleteAkSk(ctx context.Context, in *pb.DeleteAkSkRequest) error
	GetAkSk(ctx context.Context, in *pb.GetAkSkRequest) (*model.AkSkListOut, error)
	DownloadAkSk(ctx context.Context, in *pb.GetAkSkRequest) (*model.AkSkListOut, error)
}

var CredStore IAMAuthenticator

func Init(iam *config.CredentialStore) {
	switch iam.Driver {
	case "keystone":
		CredStore = keystone.Init(iam.Host)
		log.Info("Initializing Keystone...")
		return
	default:
		log.Warning("Can't find Credentials driver %s!\n", iam.Driver)
	}
}
