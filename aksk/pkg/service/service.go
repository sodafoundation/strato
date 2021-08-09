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

	log "github.com/sirupsen/logrus"

	"github.com/opensds/multi-cloud/aksk/pkg/iam"
	"github.com/opensds/multi-cloud/aksk/pkg/model"
	pb "github.com/opensds/multi-cloud/aksk/proto"
)

type akskService struct {
}

func NewAkSkService() pb.AkSkHandler {
	return &akskService{}
}

func (b *akskService) CreateAkSk(ctx context.Context, in *pb.AkSkCreateRequest, out *pb.AkSkBlob) error {
	log.Info("Received CreateAkSk request.")

	aksk := &model.AkSk{
		ProjectId: in.ProjectId,
		UserId:    in.UserId,
	}

	res, err := iam.CredStore.CreateAkSk(aksk, in)
	if err != nil {
		log.Errorf("Failed to create AKSK", err)
		return err
	}
	out.AccessKey = res.Access
	out.SecretKey = res.Secret
	log.Info("Created AKSK successfully.")
	return nil
}

func (b *akskService) GetAkSk(ctx context.Context, request *pb.GetAkSkRequest, response *pb.GetAkSkResponse) error {
	log.Info("Received GetAkSk request.")

	res, err := iam.CredStore.GetAkSk(ctx, request)
	if err != nil {
		log.Errorf("Failed to get AKSK : %v", err)
		return err
	}

	for _, cred := range res {

		aksk := &pb.AkSkDetail{}
		tmpBlob := &pb.AkSkBlob{}
		tmpBlob.AccessKey = cred.Blob.Access
		tmpBlob.SecretKey = cred.Blob.Secret

		aksk = &pb.AkSkDetail{
			ProjectId: cred.ProjectID,
			UserId:    cred.UserID,
			Blob:      tmpBlob,
			Type:      cred.Type,
		}
		response.AkSkDetail = append(response.AkSkDetail, aksk)

	}

	log.Info("Got AKSK successfully. ")
	return nil

}

func (b *akskService) DeleteAkSk(ctx context.Context, in *pb.DeleteAkSkRequest, out *pb.DeleteAkSkResponse) error {
	log.Info("Received DeleteAkSk request.")
	err := iam.CredStore.DeleteAkSk(ctx, in)
	if err != nil {
		log.Errorf("Failed to delete AKSK : %v", err)
		return err
	}

	log.Info("Deleted AKSK successfully. ")
	return nil
}

func (b *akskService) DownloadAkSk(ctx context.Context, request *pb.GetAkSkRequest, response *pb.GetAkSkResponse) error {
	log.Info("Received GetAkSk request.")

	res, err := iam.CredStore.GetAkSk(ctx, request)
	if err != nil {
		log.Errorf("Failed to get AKSK : %v", err)
		return err
	}

	for _, cred := range res {

		aksk := &pb.AkSkDetail{}
		tmpBlob := &pb.AkSkBlob{}
		tmpBlob.AccessKey = cred.Blob.Access
		tmpBlob.SecretKey = cred.Blob.Secret

		aksk = &pb.AkSkDetail{
			ProjectId: cred.ProjectID,
			UserId:    cred.UserID,
			Blob:      tmpBlob,
			Type:      cred.Type,
		}
		response.AkSkDetail = append(response.AkSkDetail, aksk)
	}

	log.Info("Downloaded AKSK successfully. ")
	return nil

}
