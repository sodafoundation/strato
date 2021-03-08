// Copyright 2019 The OpenSDS Authors.
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

package pkg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/opensds/multi-cloud/dataflow/pkg/db"
	"github.com/opensds/multi-cloud/dataflow/pkg/job"
	"github.com/opensds/multi-cloud/dataflow/pkg/kafka"
	"github.com/opensds/multi-cloud/dataflow/pkg/model"
	"github.com/opensds/multi-cloud/dataflow/pkg/plan"
	"github.com/opensds/multi-cloud/dataflow/pkg/policy"
	"github.com/opensds/multi-cloud/dataflow/pkg/utils"
	. "github.com/opensds/multi-cloud/dataflow/pkg/utils"
	pb "github.com/opensds/multi-cloud/dataflow/proto"
	log "github.com/sirupsen/logrus"
)

type dataflowService struct{}

func NewDataFlowService() pb.DataFlowHandler {
	host := os.Getenv("DB_HOST")
	dbstor := Database{Credential: "unkonwn", Driver: "mongodb", Endpoint: host}
	db.Init(&dbstor)

	addrs := []string{}
	config := strings.Split(os.Getenv("KAFKA_ADVERTISED_LISTENERS"), ";")
	for i := 0; i < len(config); i++ {
		addr := strings.Split(config[i], "//")
		if len(addr) != 2 {
			log.Info("Invalid addr:", config[i])
		} else {
			addrs = append(addrs, addr[1])
		}
	}

	kafka.Init(addrs)

	return &dataflowService{}
}

func policyModel2Resp(policy *model.Policy) *pb.Policy {
	return &pb.Policy{
		Id:          policy.Id.Hex(),
		Name:        policy.Name,
		Description: policy.Description,
		Tenant:      policy.TenantId,
		UserId:      policy.UserId,
		Schedule: &pb.Schedule{
			Type:             policy.Schedule.Type,
			TiggerProperties: policy.Schedule.TriggerProperties,
		},
	}
}

func (b *dataflowService) GetPolicy(ctx context.Context, in *pb.GetPolicyRequest, out *pb.GetPolicyResponse) error {
	log.Info("Get policy is called in dataflow service.")

	id := in.GetId()
	if id == "" {
		return errors.New("No id provided.")
	}

	p, err := policy.Get(ctx, id)
	if err != nil {
		return err
	}
	out.Policy = policyModel2Resp(p)

	//For debug -- begin
	jsons1, errs1 := json.Marshal(out)
	if errs1 != nil {
		log.Infof(errs1.Error())
	} else {
		log.Infof("jsons1: %s.\n", jsons1)
	}
	//For debug -- end
	return err
}

func (b *dataflowService) ListPolicy(ctx context.Context, in *pb.ListPolicyRequest, out *pb.ListPolicyResponse) error {
	log.Info("List policy is called in dataflow service.")

	pols, err := policy.List(ctx)
	if err != nil {
		log.Infof("List policy err:%s.", err)
		return nil
	}

	for _, p := range pols {
		out.Policies = append(out.Policies, policyModel2Resp(&p))
	}

	//For debug -- begin
	jsons1, errs1 := json.Marshal(out)
	if errs1 != nil {
		log.Infof(errs1.Error())
	} else {
		log.Infof("jsons1: %s.\n", jsons1)
	}
	//For debug -- end
	return err
}

func (b *dataflowService) CreatePolicy(ctx context.Context, in *pb.CreatePolicyRequest,
	out *pb.CreatePolicyResponse) error {
	log.Info("Create policy is called in dataflow service.")
	pol := model.Policy{}
	pol.Name = in.Policy.GetName()
	pol.Description = in.Policy.GetDescription()
	if in.Policy.GetSchedule() != nil {
		pol.Schedule.Type = in.Policy.Schedule.Type
		pol.Schedule.TriggerProperties = in.Policy.Schedule.TiggerProperties
	} else {
		out.Err = "get schedule failed"
		return errors.New("get schedule failed")
	}

	if pol.Name == "" {
		out.Err = "no name provided."
		return errors.New("et schedule failed")
	}

	pol.TenantId = in.Policy.TenantId
	pol.UserId = in.Policy.UserId
	log.Infof("dataflowservice CreatePolicy:%+v\n", pol)
	p, err := policy.Create(ctx, &pol)
	if err != nil {
		log.Infof("create policy err:%s.", out.Err)
		return nil
	}

	out.Policy = policyModel2Resp(p)
	return nil
}

func (b *dataflowService) DeletePolicy(ctx context.Context, in *pb.DeletePolicyRequest, out *pb.DeletePolicyResponse) error {
	log.Info("Delete policy is called in dataflow service.")
	id := in.GetId()
	if id == "" {
		out.Err = "Get id failed."
		return errors.New("Get id failed.")
	}

	err := policy.Delete(ctx, id)
	if err == nil {
		out.Err = ""
	} else {
		out.Err = err.Error()
	}
	log.Infof("Delete policy err:%s.", out.Err)

	return err
}

func (b *dataflowService) UpdatePolicy(ctx context.Context, in *pb.UpdatePolicyRequest, out *pb.UpdatePolicyResponse) error {
	log.Info("Update policy is called in dataflow service.")

	policyId := in.GetPolicyId()
	if policyId == "" {
		return errors.New("no id provided.")
	}

	log.Infof("body:%s", in.GetBody())
	updateMap := map[string]interface{}{}
	if err := json.Unmarshal([]byte(in.GetBody()), &updateMap); err != nil {
		return err
	}

	p, err := policy.Update(ctx, policyId, updateMap)
	if err != nil {
		log.Infof("Update policy finished, err:%s", err)
		return err
	}
	out.Policy = policyModel2Resp(p)
	return nil
}

func fillRspConnector(out *pb.Connector, in *model.Connector) {
	switch in.StorType {
	case model.STOR_TYPE_OPENSDS:
		out.BucketName = in.BucketName
	case model.STOR_TYPE_AWS_S3, model.STOR_TYPE_HW_OBS, model.STOR_TYPE_HW_FUSIONSTORAGE, model.STOR_TYPE_HW_FUSIONCLOUD,
		model.STOR_TYPE_AZURE_BLOB, model.STOR_TYPE_CEPH_S3, model.STOR_TYPE_GCP_S3, model.STOR_TYPE_ALIBABA_OSS, model.STOR_TYPE_SONY_ODA:
		for i := 0; i < len(in.ConnConfig); i++ {
			out.ConnConfig = append(out.ConnConfig, &pb.KV{Key: in.ConnConfig[i].Key, Value: in.ConnConfig[i].Value})
		}
	default:
		log.Infof("Not support connector type:%v\n", in.StorType)
	}
}

func planModel2Resp(plan *model.Plan) *pb.Plan {

	resp := &pb.Plan{
		Id:            string(plan.Id.Hex()),
		Name:          plan.Name,
		Description:   plan.Description,
		Type:          plan.Type,
		PolicyId:      plan.PolicyId,
		PolicyName:    plan.PolicyName,
		PolicyEnabled: plan.PolicyEnabled,
		RemainSource:  plan.RemainSource,
		TenantId:      plan.TenantId,
		UserId:        plan.UserId,
	}

	srcConn := pb.Connector{StorType: plan.SourceConn.StorType}
	fillRspConnector(&srcConn, &plan.SourceConn)
	destConn := pb.Connector{StorType: plan.DestConn.StorType}
	fillRspConnector(&destConn, &plan.DestConn)

	filter := pb.Filter{Prefix: plan.Filter.Prefix}

	for _, t := range plan.Filter.Tag {
		tag := &pb.KV{Key: t.Key, Value: t.Value}
		filter.Tag = append(filter.Tag, tag)
	}
	resp.SourceConn = &srcConn
	resp.DestConn = &destConn
	resp.Filter = &filter
	return resp
}

func (b *dataflowService) GetPlan(ctx context.Context, in *pb.GetPlanRequest, out *pb.GetPlanResponse) error {
	log.Info("Get plan is called in dataflow service.")

	id := in.GetId()
	if id == "" {
		errmsg := fmt.Sprint("no id specified.")
		out.Err = errmsg
		return errors.New(errmsg)
	}

	p, err := plan.Get(ctx, id)
	if err != nil {
		log.Infof("Get plan err:%s.", err)
		return err
	}

	out.Plan = planModel2Resp(p)

	//For debug -- begin
	jsons, errs := json.Marshal(out)
	if errs != nil {
		log.Infof(errs.Error())
	} else {
		log.Infof("jsons1: %s.\n", jsons)
	}
	//For debug -- end
	return err
}

func (b *dataflowService) ListPlan(ctx context.Context, in *pb.ListPlanRequest, out *pb.ListPlanResponse) error {
	log.Info("List plans is called in dataflow service.")

	if in.Limit < 0 || in.Offset < 0 {
		msg := fmt.Sprintf("Invalid pagination parameter, limit = %d and offset = %d.", in.Limit, in.Offset)
		log.Info(msg)
		return errors.New(msg)
	}

	plans, err := plan.List(ctx, int(in.Limit), int(in.Offset), in.Filter)
	if err != nil {
		log.Infof("List plans err:%s.", err)
		return err
	}

	for _, p := range plans {
		out.Plans = append(out.Plans, planModel2Resp(&p))
	}
	out.Next = in.Offset + int32(len(plans))
	//For debug -- begin
	jsons, errs := json.Marshal(out)
	if errs != nil {
		log.Infof(errs.Error())
	} else {
		log.Infof("jsons1: %s.\n", jsons)
	}
	//For debug -- end

	return err
}

func fillReqConnector(out *model.Connector, in *pb.Connector) error {
	switch in.StorType {
	case model.STOR_TYPE_OPENSDS:
		out.BucketName = in.BucketName
		return nil
	case model.STOR_TYPE_AWS_S3, model.STOR_TYPE_HW_OBS, model.STOR_TYPE_HW_FUSIONSTORAGE, model.STOR_TYPE_HW_FUSIONCLOUD,
		model.STOR_TYPE_AZURE_BLOB, model.STOR_TYPE_CEPH_S3, model.STOR_TYPE_GCP_S3, model.STOR_TYPE_IBM_COS, model.STOR_TYPE_ALIBABA_OSS, model.STOR_TYPE_SONY_ODA:
		for i := 0; i < len(in.ConnConfig); i++ {
			out.ConnConfig = append(out.ConnConfig, model.KeyValue{Key: in.ConnConfig[i].Key, Value: in.ConnConfig[i].Value})
		}
		return nil
	default:
		log.Infof("Not support connector type:%v\n", in.StorType)
		return errors.New("Invalid connector type.")
	}
}

func (b *dataflowService) CreatePlan(ctx context.Context, in *pb.CreatePlanRequest, out *pb.CreatePlanResponse) error {
	log.Info("Create plan is called in dataflow service.")

	pl := model.Plan{}
	pl.Name = in.Plan.GetName()

	pl.Description = in.Plan.GetDescription()
	pl.Type = in.Plan.GetType()
	pl.RemainSource = in.Plan.GetRemainSource()
	pl.PolicyId = in.Plan.GetPolicyId()
	pl.PolicyEnabled = in.Plan.GetPolicyEnabled()
	pl.UserId = in.Plan.UserId
	pl.TenantId = in.Plan.TenantId

	if in.Plan.GetSourceConn() != nil {
		srcConn := model.Connector{StorType: in.Plan.SourceConn.StorType}
		err := fillReqConnector(&srcConn, in.Plan.SourceConn)
		if err == nil {
			pl.SourceConn = srcConn
		} else {
			return err
		}
	} else {
		out.Err = "get source connector failed"
		return errors.New("invalid source connector")
	}
	if in.Plan.GetDestConn() != nil {
		destConn := model.Connector{StorType: in.Plan.DestConn.StorType}
		err := fillReqConnector(&destConn, in.Plan.DestConn)
		if err == nil {
			pl.DestConn = destConn
		} else {
			out.Err = err.Error()
			return err
		}
	} else {
		out.Err = "get destination connector failed"
		return errors.New("invalid destination connector")
	}
	if in.Plan.GetFilter() != nil {
		if in.Plan.Filter.Prefix != "" {
			pl.Filter = model.Filter{Prefix: in.Plan.Filter.Prefix}
		}
		if len(in.Plan.Filter.Tag) > 0 {
			for j := 0; j < len(in.Plan.Filter.Tag); j++ {
				pl.Filter.Tag = append(pl.Filter.Tag, model.KeyValue{Key: in.Plan.Filter.Tag[j].Key, Value: in.Plan.Filter.Tag[j].Value})
			}
		}
	}

	if pl.Name == "" || pl.Type == "" {
		out.Err = "Name or type is null."
		return errors.New("Name or type is null.")
	}

	p, err := plan.Create(ctx, &pl)
	if err != nil {
		log.Infof("Create plan failed, err:%v", err)
		return err
	}

	out.Plan = planModel2Resp(p)
	return nil
}

func (b *dataflowService) DeletePlan(ctx context.Context, in *pb.DeletePlanRequest, out *pb.DeletePlanResponse) error {
	log.Info("Delete plan is called in dataflow service.")

	id := in.GetId()
	if id == "" {
		out.Err = "Get id failed."
		return errors.New("Get id failed.")
	}

	err := plan.Delete(ctx, id)
	if err == nil {
		out.Err = ""
	} else {
		out.Err = err.Error()
	}
	log.Infof("Delete plan err:%s.", out.Err)

	return err
}

func (b *dataflowService) UpdatePlan(ctx context.Context, in *pb.UpdatePlanRequest, out *pb.UpdatePlanResponse) error {
	log.Info("Update plan is called in dataflow service.")

	if in.GetPlanId() == "" {
		return errors.New("No id provided.")
	}

	updateMap := map[string]interface{}{}
	if err := json.Unmarshal([]byte(in.GetBody()), &updateMap); err != nil {
		return err
	}

	p, err := plan.Update(ctx, in.GetPlanId(), updateMap)
	if err != nil {
		log.Infof("Update plan finished, err:%s.", err)
		return err
	}

	out.Plan = planModel2Resp(p)
	return nil
}

func (b *dataflowService) RunPlan(ctx context.Context, in *pb.RunPlanRequest, out *pb.RunPlanResponse) error {
	log.Info("Run plan is called in dataflow service.")

	tenantId, err := utils.GetTenantId(ctx)
	if err != nil {
		log.Errorf("run plan failed, err=%v\n", err)
		return err
	}
	userId, err := utils.GetUserId(ctx)
	if err != nil {
		log.Errorf("run plan failed, err=%v\n", err)
		return err
	}
	jid, err := plan.Run(in.Id, tenantId, userId)
	if err == nil {
		out.JobId = string(jid.Hex())
		out.Err = ""
	} else {
		out.JobId = ""
		out.Err = err.Error()
		log.Infof("Run plan err:%s.", out.Err)
	}

	return err
}

func (b *dataflowService) GetJob(ctx context.Context, in *pb.GetJobRequest, out *pb.GetJobResponse) error {
	log.Info("Get job is called in dataflow service.")

	if in.Id == "" {
		errmsg := fmt.Sprint("No id specified.")
		out.Err = errmsg
		return errors.New(errmsg)
	}

	jb, err := job.Get(ctx, in.Id)
	if err != nil {
		log.Infof("Get job err:%d.", err)
		out.Err = err.Error()
		return err
	} else {
		out.Job = &pb.Job{Id: string(jb.Id.Hex()), Type: jb.Type, PlanName: jb.PlanName, PlanId: jb.PlanId,
			Description: "for test", SourceLocation: jb.SourceLocation, DestLocation: jb.DestLocation,
			CreateTime: jb.CreateTime.Unix(), EndTime: jb.EndTime.Unix(), Status: jb.Status, TotalCapacity: jb.TotalCapacity,
			PassedCapacity: jb.PassedCapacity, TotalCount: jb.TotalCount, PassedCount: jb.PassedCount, Progress: jb.Progress}
	}

	//For debug -- begin
	jsons, errs := json.Marshal(out)
	if errs != nil {
		log.Infof(errs.Error())
	} else {
		log.Infof("jsons1: %s.\n", jsons)
	}
	//For debug -- end
	return err
}

func (b *dataflowService) ListJob(ctx context.Context, in *pb.ListJobRequest, out *pb.ListJobResponse) error {
	log.Info("List job is called in dataflow service.")
	if in.Limit < 0 || in.Offset < 0 {
		msg := fmt.Sprintf("invalid pagination parameter, limit = %d and offset = %d.", in.Limit, in.Offset)
		log.Info(msg)
		return errors.New(msg)
	}

	jobs, err := job.List(ctx, int(in.Limit), int(in.Offset), in.Filter)
	if err != nil {
		log.Infof("Get job err:%d.", err)
		return err
	}

	if err == nil {
		//for i := 0; i < len(jobs); i++ {
		for _, job := range jobs {
			//TODO: need change according to real scenario
			j := pb.Job{Id: string(job.Id.Hex()), Type: job.Type, PlanName: job.PlanName, PlanId: job.PlanId,
				SourceLocation: job.SourceLocation, DestLocation: job.DestLocation, StartTime: job.StartTime.Unix(),
				CreateTime: job.CreateTime.Unix(), EndTime: job.EndTime.Unix(), Status: job.Status,
				TotalCapacity: job.TotalCapacity, PassedCapacity: job.PassedCapacity, TotalCount: int64(job.TotalCount),
				PassedCount: (int64(job.PassedCount)), Progress: int64(job.Progress)}
			out.Jobs = append(out.Jobs, &j)
		}
	}

	out.Next = in.Offset + int32(len(jobs))
	//For debug -- begin
	jsons, errs := json.Marshal(out)
	if errs != nil {
		log.Infof(errs.Error())
	} else {
		log.Infof("Got jobs: %s.\n", jsons)
	}
	//For debug -- end
	return err
}
