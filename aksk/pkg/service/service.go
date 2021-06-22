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
	"fmt"

	"github.com/opensds/multi-cloud/aksk/pkg/iam"
	"github.com/opensds/multi-cloud/aksk/pkg/model"
	pb "github.com/opensds/multi-cloud/aksk/proto"

	log "github.com/sirupsen/logrus"
)

type akskService struct {
}

func NewAkSkService() pb.AkSkHandler {
	return &akskService{}
}

func (b akskService) CreateAkSk(ctx context.Context, in *pb.CreateAkSkRequest, out *pb.CreateAkSkResponse) error {
	log.Info("Received CreateAkSk request.")

	aksk := &model.AkSk{
		ProjectId: in.Aksk.ProjectId,
		UserId:    in.Aksk.UserId,
		Type:      in.Aksk.Type,
		Blob:      in.Aksk.Blob,
	}

	res, err := iam.CredStore.CreateAkSk(aksk, in)
	if err != nil {
		log.Errorf("Failed to create AKSK : %v", err)
		return err
	}

	out.Aksk = &pb.AkSkDetail{
		ProjectId: res.Credential.ProjectID,
		UserId:    res.Credential.UserID,
		Type:      res.Credential.Type,
		Blob:      res.Credential.Blob,
	}

	log.Info("Created AKSK successfully. Response  : ", res.Credential.Blob)
	return nil
}

func (b akskService) GetAkSk(ctx context.Context, request *pb.GetAkSkRequest, response *pb.GetAkSkResponse) error {
	log.Info("Received GetAkSk request.")

	res, err := iam.CredStore.GetAkSk(ctx, request)
	if err != nil {
		log.Errorf("Failed to get AKSK : %v", err)
		return err
	}

	aksk := &pb.AkSkDetail{}
	for idx, cred := range res.Credentials {
		aksk = &pb.AkSkDetail{
			ProjectId: cred.ProjectID,
			UserId:    cred.UserID,
			Blob:      cred.Blob,
			Type:      cred.Type,
		}
		response.AkSkDetail = append(response.AkSkDetail, aksk)
		fmt.Println("At index", idx, "value is", cred)
	}

	log.Info("Got AKSK successfully. ", response)
	return nil

}

func (b akskService) DeleteAkSk(ctx context.Context, in *pb.DeleteAkSkRequest, out *pb.DeleteAkSkResponse) error {
	log.Info("Received DeleteAkSk request.")
	err := iam.CredStore.DeleteAkSk(ctx, in) //DeleteAKSK(ctx, in)
	if err != nil {
		log.Errorf("Failed to delete AKSK : %v", err)
		return err
	}

	log.Info("Deleted AKSK successfully. ")
	return nil
}

func (b akskService) DownloadAkSk(ctx context.Context, request *pb.GetAkSkRequest, response *pb.GetAkSkResponse) error {
	log.Info("Received GetAkSk request.")

	res, err := iam.CredStore.GetAkSk(ctx, request)
	if err != nil {
		log.Errorf("Failed to get AKSK : %v", err)
		return err
	}

	aksk := &pb.AkSkDetail{}
	for idx, cred := range res.Credentials {
		aksk = &pb.AkSkDetail{
			ProjectId: cred.ProjectID,
			UserId:    cred.UserID,
			Blob:      cred.Blob,
			Type:      cred.Type,
		}
		response.AkSkDetail = append(response.AkSkDetail, aksk)
		fmt.Println("At index", idx, "value is", cred)
	}

	log.Info("Downloaded AKSK successfully. ", response)
	return nil

}
