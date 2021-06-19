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

package service

import (
	"context"
	"github.com/opensds/multi-cloud/aksk/pkg/model"

	keystone "github.com/opensds/multi-cloud/aksk/pkg/keystone"
	pb "github.com/opensds/multi-cloud/aksk/proto"
	log "github.com/sirupsen/logrus"
)

type AkSkService struct {
}

func NewAkSkService() pb.AkSkHandler {
	return &AkSkService{}
}

func (b *AkSkService) CreateAkSk(ctx context.Context, in *pb.CreateAkSkRequest, out *pb.CreateAkSkResponse) error {
	log.Info("Received CreateAkSk request.")

	aksk := &model.AkSk{
		ProjectId: in.Aksk.ProjectId,
		UserId: in.Aksk.UserId,
		Type: in.Aksk.Type,
		Blob: in.Aksk.Blob,
	}

	res, err := keystone.CreateAKSK(aksk)
	if err != nil {
		log.Errorf("Failed to create AKSK : %v", err)
		return err
	}

	log.Info("Created AKSK successfully. Response Status : " , res)
	return nil

}

func (b *AkSkService) GetAkSk(ctx context.Context, in *pb.GetAkSkRequest, out *pb.GetAkSkResponse) error {
	log.Info("Received GetAkSk request.")

	return nil
}

func (b *AkSkService) ListAkSk(ctx context.Context, in *pb.ListAkSkRequest, out *pb.ListAkSkResponse) error {
	log.Info("Received ListAkSk request.")

	return nil
}

func (b *AkSkService) DeleteAkSk(ctx context.Context, in *pb.DeleteAkSkRequest, out *pb.DeleteAkSkResponse) error {
	log.Info("Received DeleteAkSk request.")

	return nil
}
