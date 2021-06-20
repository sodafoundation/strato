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
	"github.com/opensds/multi-cloud/aksk/pkg/iam"
	"github.com/opensds/multi-cloud/aksk/pkg/model"
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

	res, err := iam.CredStore.CreateAkSk(ctx, aksk)
	if err != nil {
		log.Errorf("Failed to create AKSK : %v", err)
		return err
	}

	out.Aksk = &pb.AkSkDetail{
		ProjectId: res.ProjectId,
		UserId: res.UserId,
		Type: res.Type,
		Blob: res.Blob,
	}

	log.Info("Created AKSK successfully. Response Status : " , res)
	return nil
}

func (b *AkSkService) GetAkSk(ctx context.Context, in *pb.GetAkSkRequest, out *pb.GetAkSkResponse) error {
	log.Info("Received GetAkSk request.")

	res, err := iam.CredStore.GetAkSk(ctx, in) //GetAKSK(ctx, in)
	if err != nil {
		log.Errorf("Failed to get AKSK : %v", err)
		return err
	}

	out.AkSkDetail = &pb.AkSkDetail{
		ProjectId: res.ProjectId,
		UserId: res.UserId,
		Type: res.Type,
		Blob: res.Blob,
	}

	/*bodyString := ""
	if res.StatusCode == http.StatusOK {
		bodyBytes, err := ioutil.ReadAll(res.Body)
		if err != nil {
			log.Fatal(err)
		}
		bodyString = string(bodyBytes)
		log.Info(bodyString)
	}*/

	log.Info("Created AKSK successfully. " , out)
	return nil

}
/*
func (b *AkSkService) ListAkSk(ctx context.Context, in *pb.ListAkSkRequest, out *pb.ListAkSkResponse) error {
	log.Info("Received ListAkSk request.")

	if in.Limit < 0 || in.Offset < 0 {
		msg := fmt.Sprintf("invalid pagination parameter, limit = %d and offset = %d.", in.Limit, in.Offset)
		log.Info(msg)
		return errors.New(msg)
	}

	res, err := keystone.ListAKSK(ctx)
	if err != nil {
		log.Errorf("failed to list backend: %v\n", err)
		return err
	}

	/*var aksks []*pb.AkSkDetail
	for _, item := range res {
		aksks = append(aksks, &pb.AkSkDetail{
		ProjectId: item.ProjectId,

		})
	}
	bodyString := ""
	if res.StatusCode == http.StatusOK {
		bodyBytes, err := ioutil.ReadAll(res.Body)
		if err != nil {
			log.Fatal(err)
		}
		bodyString = string(bodyBytes)
		log.Info(bodyString)
	}
	//out.Next = in.Offset + int32(len(res))

	log.Infof("List AKSK successful, aksks:%+v\n", bodyString)
	return nil
}*/

func (b *AkSkService) DeleteAkSk(ctx context.Context, in *pb.DeleteAkSkRequest, out *pb.DeleteAkSkResponse) error {
	log.Info("Received DeleteAkSk request.")
	err := iam.CredStore.DeleteAkSk(ctx, in) //DeleteAKSK(ctx, in)
	if err != nil {
		log.Errorf("Failed to delete AKSK : %v", err)
		return err
	}

	log.Info("Deleted AKSK successfully. ")
	return nil
}
