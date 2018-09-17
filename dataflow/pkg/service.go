package pkg

import (
	"context"

	"github.com/micro/go-log"
	pb "github.com/opensds/go-panda/dataflow/proto"
	"github.com/opensds/go-panda/dataflow/pkg/policy"
	"github.com/opensds/go-panda/dataflow/pkg/plan"
	"github.com/opensds/go-panda/dataflow/pkg/type"
	"github.com/opensds/go-panda/dataflow/pkg/db"
	. "github.com/opensds/go-panda/dataflow/pkg/utils"
	"os"
	"encoding/json"
	"github.com/globalsign/mgo/bson"
	"github.com/opensds/go-panda/dataflow/pkg/job"
	"github.com/opensds/go-panda/datamover/proto"
	"github.com/micro/go-micro/client"
	"errors"
)

type dataflowService struct{
	datamoverClient datamover.DatamoverService
}

func NewDataFlowService() pb.DataFlowHandler {
	host := os.Getenv("DB_HOST")
	dbstor := Database{Credential:"unkonwn", Driver:"mongodb", Endpoint:host}
	db.Init(&dbstor)

	return &dataflowService{datamoverClient:datamover.NewDatamoverService("datamover", client.DefaultClient)}
}

func (b *dataflowService) GetPolicy(ctx context.Context, in *pb.GetPolicyRequest, out *pb.GetPolicyResponse) error {
	log.Log("Get policy is called in dataflow service.")

	name := in.GetName()
	if name == "" {
		out.Err = "No name provided."
		return errors.New("No name provided.")
	}
	//TODO: how to get tenant
	//tenant := in.Tenant
	tenant := "tenant"
	pols,err := policy.Get(name, tenant)
	if err == nil {
		out.Err = ""
	}else {
		out.Err = err.Error()
	}
	log.Logf("Getpolicy err:%s.", out.Err)
	//log.Logf("Getpolicy err:%d\n", rsp.ErrCode)
	if err == nil {
		for i := 0; i < len(pols); i++ {
			p := pb.Policy{Id:string(pols[i].Id.Hex()), Name:pols[i].Name, Tenant:pols[i].Tenant,
			Description:pols[i].Description}
			sched := pb.Schedule{Type:pols[i].Schedule.Type, TimePoint:pols[i].Schedule.TimePoint}
			for j := 0; j < len(pols[i].Schedule.Day); j++ {
				sched.Days = append(sched.Days, pols[i].Schedule.Day[j])
			}
			p.Sched = &sched
			out.Pols = append(out.Pols, &p)
		}
	}

	//For debug -- begin
	jsons1, errs1 := json.Marshal(out)
	if errs1 != nil {
		log.Logf(errs1.Error())
	}else {
		log.Logf("jsons1: %s.\n", jsons1)
	}
	//For debug -- end
	return err
}

func (b *dataflowService) CreatePolicy(ctx context.Context, in *pb.CreatePolicyRequest, out *pb.CreatePolicyResponse) error {
	log.Log("Create policy is called in dataflow service.")
	pol := _type.Policy{}
	pol.Name = in.Pol.GetName()
	//TODO:how to get tenant
	//pol.Tenant = in.Pol.GetTenant()
	pol.Tenant = "tenant"
	pol.Description = in.Pol.GetDescription()
	if in.Pol.GetSched() != nil {
		pol.Schedule.Day = in.Pol.Sched.Days
		pol.Schedule.TimePoint = in.Pol.Sched.TimePoint
		pol.Schedule.Type = in.Pol.Sched.Type
	}else {
		out.Err = "Get schedule failed."
		return errors.New("Get schedule failed.")
	}

	if pol.Name == "" {
		out.Err = "no name provided."
		return errors.New("Get schedule failed.")
	}

	//rsp := pb.CreatePolicyResponse{}
	err := policy.Create(&pol)
	if err == nil {
		out.PolId = string(pol.Id.Hex())
		out.Err = ""
	}else {
		out.Err = err.Error()
	}
	log.Logf("Create policy err:%s.", out.Err)

	return err
}

func (b *dataflowService) DeletePolicy(ctx context.Context, in *pb.DeletePolicyRequest, out *pb.DeletePolicyResponse) error {
	log.Log("Delete policy is called in dataflow service.")
	//out.Id = "c506cd4b-9048-43bc-97ef-0d7dec369b42"
	//out.Name = "GetPolicy." + in.Id

	id := in.GetId()
	if id == "" {
		out.Err = "Get id failed."
		return errors.New("Get id failed.")
	}
	//TODO: how to get tenant
	//tenant := in.Tenant
	tenant := "tenant"
	err := policy.Delete(id, tenant)
	if err == nil {
		out.Err = ""
	}else {
		out.Err = err.Error()
	}
	log.Logf("Delete policy err:%s.", out.Err)

	return err
}

func (b *dataflowService) UpdatePolicy(ctx context.Context, in *pb.UpdatePolicyRequest, out *pb.UpdatePolicyResponse) error {
	log.Log("Update policy is called in dataflow service.")
	pol := _type.Policy{}
	if in.Pol.GetId() == "" {
		out.Err = "No id provided."
		return errors.New("No id provided.")
	}
	pol.Id = bson.ObjectIdHex(in.Pol.Id)
	if pol.Id == "" {
		out.Err = "Get id failed."
		return errors.New("Get id failed.")
	}

	pol.Name = in.Pol.GetName()
	//TODO: how to get tenant
	//pol.Tenant = in.Pol.GetTenant()
	pol.Tenant = "tenant"
	pol.Description = in.Pol.GetDescription()
	if in.Pol.GetSched() != nil {
		pol.Schedule.Day = in.Pol.Sched.Days
		pol.Schedule.TimePoint = in.Pol.Sched.TimePoint
		pol.Schedule.Type = in.Pol.Sched.Type
	}

	//rsp := pb.CreatePolicyResponse{}
	err := policy.Update(&pol)
	if err == nil {
		out.Err = ""
		out.PolId = string(pol.Id.Hex())
	}else {
		out.Err = err.Error()
	}
	log.Logf("Update policy finished, err:%s", out.Err)

	return err
}

func fillRspConnector (out *pb.Connector, in *_type.Connector) {
	switch in.StorType {
	case _type.STOR_TYPE_OPENSDS:
		out.BucketName = in.BucketName
	default:
		log.Logf("Not support connector type:%v\n", in.StorType)
	}
}

func (b *dataflowService) GetPlan(ctx context.Context, in *pb.GetPlanRequest, out *pb.GetPlanResponse) error {
	log.Log("Get plan is called in dataflow service.")

	name := in.GetName()
	if name == ""{
		out.Err = "No name specified."
		return errors.New("No name specified.")
	}
	//TODO: how to get tenant
	//tenant := in.Tenant
	tenant := "tenant"
	pls,err := plan.Get(name, tenant)
	if err == nil {
		out.Err = ""
	}else {
		out.Err = err.Error()
	}
	log.Logf("Get plan err:%s.", out.Err)
	//log.Logf("Getpolicy err:%d\n", rsp.ErrCode)
	if err == nil {
		for i := 0; i < len(pls); i++ {
			pl := pb.Plan{Id:string(pls[i].Id.Hex()), Name:pls[i].Name, Description:pls[i].Description,
				Type:pls[i].Type, PolicyId:pls[i].PolicyId, PolicyName:pls[i].PolicyName,
				OverWrite:pls[i].OverWrite, RemainSource:pls[i].RemainSource, Tenant:pls[i].Tenant}

			srcConn := pb.Connector{StorType:pls[i].SourceConn.StorType}
			fillRspConnector(&srcConn, &pls[i].SourceConn)
			destConn := pb.Connector{StorType:pls[i].DestConn.StorType}
			fillRspConnector(&destConn, &pls[i].DestConn)

			filt := pb.Filter{Prefix:pls[i].Filt.Prefix}
			for j := 0; j < len(pls[i].Filt.Tag); j++ {
				t := pb.KV{Key:pls[i].Filt.Tag[j].Key, Value:pls[i].Filt.Tag[j].Value}
				filt.Tag = append(filt.Tag, &t)
			}

			pl.SourceConn = &srcConn
			pl.DestConn = &destConn
			pl.Filt = &filt

			out.Plans = append(out.Plans, &pl)
		}
	}

	//For debug -- begin
	jsons, errs := json.Marshal(out)
	if errs != nil {
		log.Logf(errs.Error())
	}else {
		log.Logf("jsons1: %s.\n", jsons)
	}
	//For debug -- end
	return err
}

func fillReqConnector (out *_type.Connector, in *pb.Connector) error {
	switch in.StorType {
	case _type.STOR_TYPE_OPENSDS:
		out.BucketName = in.BucketName
		return nil
	default:
		log.Logf("Not support connector type:%v\n", in.StorType)
		return errors.New("Invalid connector type.")
	}
}

func (b *dataflowService) CreatePlan(ctx context.Context, in *pb.CreatePlanRequest, out *pb.CreatePlanResponse) error {
	log.Log("Create plan is called in dataflow service.")
	pl := _type.Plan{}
	pl.Name = in.Plan.GetName()
	//TODO:get tenant
	//pl.Tenant = in.Plan.GetTenant()
	pl.Tenant = "tenant"
	pl.Description = in.Plan.GetDescription()
	pl.Type = in.Plan.GetType()
	pl.OverWrite = in.Plan.GetOverWrite()
	pl.RemainSource = in.Plan.GetRemainSource()
	pl.PolicyId = in.Plan.GetPolicyId()
	//pl.PolicyName = in.Plan.GetPolicyName()

	if in.Plan.GetSourceConn() != nil {
		srcConn := _type.Connector{StorType:in.Plan.SourceConn.StorType}
		err := fillReqConnector(&srcConn, in.Plan.SourceConn)
		if err == nil {
			pl.SourceConn = srcConn
		}else {
			return err
		}
	}else {
		out.Err = "Get source connector failed."
		return errors.New("Invalid source connector.")
	}
	if in.Plan.GetDestConn() != nil {
		destConn := _type.Connector{StorType:in.Plan.DestConn.StorType}
		err := fillReqConnector(&destConn, in.Plan.DestConn)
		if err == nil {
			pl.DestConn = destConn
		}else {
			out.Err = err.Error()
			return err
		}
	}else {
		out.Err = "Get destination connector failed."
		return errors.New("Invalid destination connector.")
	}
	if in.Plan.GetFilt() != nil {
		if in.Plan.Filt.Prefix != "" {
			pl.Filt = _type.Filter{Prefix:in.Plan.Filt.Prefix}
		}
		if len(in.Plan.Filt.Tag) > 0 {
			for j := 0; j < len(in.Plan.Filt.Tag); j++ {
				pl.Filt.Tag = append(pl.Filt.Tag, _type.KeyValue{Key:in.Plan.Filt.Tag[j].Key, Value:in.Plan.Filt.Tag[j].Value})
			}
		}
	}else {
		pl.Filt = _type.Filter{Prefix:"/"} //this is default
	}

	if pl.Name == "" || pl.Type == "" {
		out.Err = "Name or type is null."
		return errors.New("Name or type is null.")
	}

	//rsp := pb.CreatePolicyResponse{}
	log.Logf("plan:%+v\n", pl)
	err := plan.Create(&pl)
	if err == nil {
		out.Err = ""
		out.PlanId = string(pl.Id.Hex())
	}else {
		out.Err = err.Error()
	}
	log.Logf("Create plan err:%s.", out.Err)

	return err
}

func (b *dataflowService) DeletePlan(ctx context.Context, in *pb.DeletePlanRequest, out *pb.DeletePlanResponse) error {
	log.Log("Delete plan is called in dataflow service.")
	//out.Id = "c506cd4b-9048-43bc-97ef-0d7dec369b42"
	//out.Name = "GetPolicy." + in.Id

	id := in.GetId()
	if id == "" {
		out.Err = "Get id failed."
		return errors.New("Get id failed.")
	}
	//TODO: how to get tenant
	//tenant := in.Tenant
	tenant := "tenant"
	err := plan.Delete(id, tenant)
	if err == nil {
		out.Err = ""
	}else {
		out.Err = err.Error()
	}
	log.Logf("Delete plan err:%s.", out.Err)

	return err
}

func (b *dataflowService) UpdatePlan(ctx context.Context, in *pb.UpdatePlanRequest, out *pb.UpdatePlanResponse) error {
	log.Log("Update plan is called in dataflow service.")
	pl := _type.Plan{}
	if in.Plan.GetId() == "" {
		out.Err = "No id provided."
		return errors.New("No id provided.")
	}
	pl.Id = bson.ObjectIdHex(in.Plan.GetId())
	pl.Name = in.Plan.GetName()
	pl.Description = in.Plan.GetDescription()
	pl.Type = in.Plan.GetType()
	pl.OverWrite = in.Plan.GetOverWrite()
	pl.RemainSource = in.Plan.GetRemainSource()
	pl.PolicyId = in.Plan.GetPolicyId()
	//TODO:how to get tenant
	//pl.Tenant = in.Plan.GetTenant()
	pl.Tenant = "tenant"

	if in.Plan.GetSourceConn() != nil {
		srcConn := _type.Connector{StorType:in.Plan.SourceConn.StorType}
		fillReqConnector(&srcConn, in.Plan.SourceConn)
		pl.SourceConn = srcConn
	}
	if in.Plan.GetDestConn() != nil {
		destConn := _type.Connector{StorType:in.Plan.DestConn.StorType}
		fillReqConnector(&destConn, in.Plan.DestConn)
		pl.DestConn = destConn
	}
	if in.Plan.GetFilt() != nil {
		if in.Plan.Filt.Prefix != "" {
			pl.Filt = _type.Filter{Prefix:in.Plan.Filt.Prefix}
		}
		if len(in.Plan.Filt.Tag) > 0 {
			for j := 0; j < len(in.Plan.Filt.Tag); j++ {
				pl.Filt.Tag = append(pl.Filt.Tag, _type.KeyValue{Key:in.Plan.Filt.Tag[j].Key, Value:in.Plan.Filt.Tag[j].Value})
			}
		}
	}

	//TODO: Check validation of input parameter

	err := plan.Update(&pl)
	if err == nil {
		out.Err = ""
		out.PlanId = string(pl.Id.Hex())
	}else {
		out.Err = err.Error()
	}
	log.Logf("Update plan finished, err:%s.", out.Err)

	return err
}

func (b *dataflowService) RunPlan(ctx context.Context, in *pb.RunPlanRequest, out *pb.RunPlanResponse) error {
	log.Log("Run plan is called in dataflow service.")

	id := in.Id
	//TODO: how to get tenant
	//tenant := in.Tenant
	tenant := "tenant"
 	jid, err := plan.Run(id, tenant, b.datamoverClient)
	if err == nil {
		out.JobId = string(jid.Hex())
		out.Err = ""
	}else {
		out.JobId = ""
		out.Err = err.Error()
	}
	log.Logf("Run plan err:%d.", out.Err)
	return err
}

func (b *dataflowService) GetJob(ctx context.Context, in *pb.GetJobRequest, out *pb.GetJobResponse) error {
	log.Log("Get job is called in dataflow service.")

	id := in.Id
	if in.Id == "all" {
		id = ""
	}

	//TODO: how to get tenant
	//tenant := in.Tenant
	tenant := "tenant"
	js,err := job.Get(id, tenant)
	if err == nil {
		out.Err = ""
	}else {
		out.Err = err.Error()
	}
	log.Logf("Get job err:%d.", out.Err)
	//log.Logf("Getpolicy err:%d\n", rsp.ErrCode)
	if err == nil {
		for i := 0; i < len(js); i++ {
			//des := "Total Capacity:" + js[i].TotalCapacity + ", "
			//TODO: need change according to real scenario
			des := "for test"
			job := pb.Job{Id:string(js[i].Id.Hex()), Type:js[i].Type, PlanName:js[i].PlanName, PlanId:js[i].PlanId,
				Description:des, SourceLocation:js[i].SourceLocation, DestLocation:js[i].DestLocation,
				CreateTime:js[i].CreateTime.Unix(), EndTime:js[i].EndTime.Unix()}
			out.Jobs = append(out.Jobs, &job)
		}
	}

	//For debug -- begin
	jsons, errs := json.Marshal(out)
	if errs != nil {
		log.Logf(errs.Error())
	}else {
		log.Logf("jsons1: %s.\n", jsons)
	}
	//For debug -- end
	return err
}
