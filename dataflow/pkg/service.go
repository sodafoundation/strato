package pkg

import (
	"context"

	"github.com/micro/go-log"
	pb "github.com/opensds/go-panda/dataflow/proto"
	"github.com/opensds/go-panda/dataflow/pkg/policy"
	"github.com/opensds/go-panda/dataflow/pkg/connector"
	"github.com/opensds/go-panda/dataflow/pkg/plan"
	"github.com/opensds/go-panda/dataflow/pkg/type"
	"github.com/opensds/go-panda/dataflow/pkg/db"
	. "github.com/opensds/go-panda/dataflow/pkg/utils"
	"os"
	"encoding/json"
	"github.com/globalsign/mgo/bson"
	"github.com/opensds/go-panda/dataflow/pkg/job"
)

type dataflowService struct{}

func NewDataFlowService() pb.DataFlowHandler {
	host := os.Getenv("DB_HOST")
	dbstor := Database{Credential:"unkonwn", Driver:"mongodb", Endpoint:host}
	db.Init(&dbstor)
	return &dataflowService{}
}

func (b *dataflowService) GetPolicy(ctx context.Context, in *pb.GetPolicyRequest, out *pb.GetPolicyResponse) error {
	log.Log("Get policy is called in dataflow service.")
	//out.Id = "c506cd4b-9048-43bc-97ef-0d7dec369b42"
	//out.Name = "GetPolicy." + in.Id

	name := in.Name
	//TO-DO: how to get tenant
	//tenant := in.Tenant
	tenant := "tenant"
	pols,err := policy.Get(name, tenant)
	out.ErrCode = int64(err)
	log.Logf("Getpolicy err:%d.", out.ErrCode)
	//fmt.Printf("Getpolicy err:%d\n", rsp.ErrCode)
	if err == _type.ERR_OK {
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
	return nil
}

func (b *dataflowService) CreatePolicy(ctx context.Context, in *pb.CreatePolicyRequest, out *pb.CreatePolicyResponse) error {
	log.Log("Create policy is called in dataflow service.")
	pol := _type.Policy{}
	pol.Name = in.Pol.Name
	pol.Tenant = in.Pol.Tenant
	pol.Description = in.Pol.Description
	pol.Schedule.Day = in.Pol.Sched.Days
	pol.Schedule.TimePoint = in.Pol.Sched.TimePoint
	pol.Schedule.Type = in.Pol.Sched.Type

	//rsp := pb.CreatePolicyResponse{}
	errcode := policy.Create(&pol)
	out.ErrCode = int64(errcode)

	return nil
}

func (b *dataflowService) DeletePolicy(ctx context.Context, in *pb.DeletePolicyRequest, out *pb.DeletePolicyResponse) error {
	log.Log("Delete policy is called in dataflow service.")
	//out.Id = "c506cd4b-9048-43bc-97ef-0d7dec369b42"
	//out.Name = "GetPolicy." + in.Id

	id := in.Id
	//TO-DO: how to get tenant
	//tenant := in.Tenant
	tenant := "tenant"
	errode := policy.Delete(id, tenant)
	out.ErrCode = int64(errode)
	log.Logf("Delete policy err:%d.", out.ErrCode)

	return nil
}

func (b *dataflowService) UpdatePolicy(ctx context.Context, in *pb.UpdatePolicyRequest, out *pb.UpdatePolicyResponse) error {
	log.Log("Update policy is called in dataflow service.")
	pol := _type.Policy{}
	pol.Id = bson.ObjectIdHex(in.Pol.Id)
	pol.Name = in.Pol.Name
	pol.Tenant = in.Pol.Tenant
	pol.Description = in.Pol.Description
	pol.Schedule.Day = in.Pol.Sched.Days
	pol.Schedule.TimePoint = in.Pol.Sched.TimePoint
	pol.Schedule.Type = in.Pol.Sched.Type

	//rsp := pb.CreatePolicyResponse{}
	errcode := policy.Update(&pol)
	log.Logf("Update policy finished, err:%d", errcode)
	out.ErrCode = int64(errcode)

	return nil
}

func (b *dataflowService) GetConnector(ctx context.Context, in *pb.GetConnectorRequest, out *pb.GetConnectorResponse) error {
	log.Log("Get connector is called in dataflow service.")

	name := in.Name
	//TO-DO: how to get tenant
	//tenant := in.Tenant
	tenant := "tenant"
	conns,err := connector.Get(name, tenant)
	out.ErrCode = int64(err)
	log.Logf("Get connector err:%d.", out.ErrCode)
	//fmt.Printf("Getpolicy err:%d\n", rsp.ErrCode)
	if err == _type.ERR_OK {
		for i := 0; i < len(conns); i++ {
			c := pb.Connector{Id:string(conns[i].Id.Hex()), Name:conns[i].Name, SelfDef:conns[i].SelfDef,
				BucketName:conns[i].BucketName, StorType:conns[i].StorType, StorLocation:conns[i].StorLocation,
				AccessKey:conns[i].AccessKey, SecreteKey:conns[i].SecreteKey, UserName:conns[i].UserName,
				Passwd:conns[i].Passwd, Tenant:conns[i].Tenant}
			out.Conns = append(out.Conns, &c)
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
	return nil
}

func (b *dataflowService) CreateConnector(ctx context.Context, in *pb.CreateConnectorRequest, out *pb.CreateConnectorResponse) error {
	log.Log("Create connector is called in dataflow service.")
	con := _type.Connector{}
	con.Name = in.Conn.Name
	con.Tenant = in.Conn.Tenant
	con.SelfDef = in.Conn.SelfDef
	con.BucketName = in.Conn.BucketName
	con.StorType = in.Conn.StorType
	con.StorLocation = in.Conn.StorLocation
	con.AccessKey = in.Conn.AccessKey
	con.SecreteKey = in.Conn.SecreteKey
	con.UserName = in.Conn.UserName
	con.Passwd = in.Conn.Passwd

	//rsp := pb.CreatePolicyResponse{}
	errcode := connector.Create(&con)
	out.ErrCode = int64(errcode)

	return nil
}

func (b *dataflowService) DeleteConnector(ctx context.Context, in *pb.DeleteConnectorRequest, out *pb.DeleteConnectorResponse) error {
	log.Log("Delete connector is called in dataflow service.")
	//out.Id = "c506cd4b-9048-43bc-97ef-0d7dec369b42"
	//out.Name = "GetPolicy." + in.Id

	id := in.Id
	//TO-DO: how to get tenant
	//tenant := in.Tenant
	tenant := "tenant"
	errode := connector.Delete(id, tenant)
	out.ErrCode = int64(errode)
	log.Logf("Delete connector err:%d.", out.ErrCode)

	return nil
}

func (b *dataflowService) UpdateConnector(ctx context.Context, in *pb.UpdateConnectorRequest, out *pb.UpdateConnectorResponse) error {
	log.Log("Update connector is called in dataflow service.")
	con := _type.Connector{}
	con.Id = bson.ObjectIdHex(in.Conn.Id)
	con.Name = in.Conn.Name
	con.Tenant = in.Conn.Tenant
	con.SelfDef = in.Conn.SelfDef
	con.BucketName = in.Conn.BucketName
	con.StorType = in.Conn.StorType
	con.StorLocation = in.Conn.StorLocation
	con.AccessKey = in.Conn.AccessKey
	con.SecreteKey = in.Conn.SecreteKey
	con.UserName = in.Conn.UserName
	con.Passwd = in.Conn.Passwd

	//rsp := pb.CreatePolicyResponse{}
	errcode := connector.Update(&con)
	log.Logf("Update connector finished, err:%d", errcode)
	out.ErrCode = int64(errcode)

	return nil
}

func (b *dataflowService) GetPlan(ctx context.Context, in *pb.GetPlanRequest, out *pb.GetPlanResponse) error {
	log.Log("Get plan is called in dataflow service.")

	name := in.Name
	//TO-DO: how to get tenant
	//tenant := in.Tenant
	tenant := "tenant"
	pls,err := plan.Get(name, tenant)
	out.ErrCode = int64(err)
	log.Logf("Get plan err:%d.", out.ErrCode)
	//fmt.Printf("Getpolicy err:%d\n", rsp.ErrCode)
	if err == _type.ERR_OK {
		for i := 0; i < len(pls); i++ {
			pl := pb.Plan{Id:string(pls[i].Id.Hex()), Name:pls[i].Name, Description:pls[i].Description,
				Type:pls[i].Type, SourceConnId:pls[i].SourceConnId, SourceConnName:pls[i].SourceConnName,
				DestConnId:pls[i].DestConnId, DestConnName:pls[i].DestConnName, PolicyId:pls[i].PolicyId,
				PolicyName:pls[i].PolicyName, SourceDir:pls[i].SourceDir, DestDir:pls[i].DestDir, OverWrite:pls[i].OverWrite,
				RemainSource:pls[i].RemainSource, Tenant:pls[i].Tenant}
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
	return nil
}

func (b *dataflowService) CreatePlan(ctx context.Context, in *pb.CreatePlanRequest, out *pb.CreatePlanResponse) error {
	log.Log("Create plan is called in dataflow service.")
	pl := _type.Plan{}
	pl.Name = in.Plan.Name
	pl.Tenant = in.Plan.Tenant
	pl.Description = in.Plan.Description
	pl.Type = in.Plan.Type
	pl.SourceConnId = in.Plan.SourceConnId
	pl.SourceConnName = in.Plan.SourceConnName
	pl.DestConnId = in.Plan.DestConnId
	pl.DestConnName = in.Plan.DestConnName
	pl.SourceDir = in.Plan.SourceDir
	pl.DestDir = in.Plan.DestDir
	pl.OverWrite = in.Plan.OverWrite
	pl.RemainSource = in.Plan.RemainSource
	pl.PolicyId = in.Plan.PolicyId
	pl.PolicyName = in.Plan.PolicyName

	//TO-DO: Check validation of input parameter

	//rsp := pb.CreatePolicyResponse{}
	errcode := plan.Create(&pl)
	out.ErrCode = int64(errcode)

	return nil
}

func (b *dataflowService) DeletePlan(ctx context.Context, in *pb.DeletePlanRequest, out *pb.DeletePlanResponse) error {
	log.Log("Delete plan is called in dataflow service.")
	//out.Id = "c506cd4b-9048-43bc-97ef-0d7dec369b42"
	//out.Name = "GetPolicy." + in.Id

	id := in.Id
	//TO-DO: how to get tenant
	//tenant := in.Tenant
	tenant := "tenant"
	errode := plan.Delete(id, tenant)
	out.ErrCode = int64(errode)
	log.Logf("Delete plan err:%d.", out.ErrCode)

	return nil
}

func (b *dataflowService) UpdatePlan(ctx context.Context, in *pb.UpdatePlanRequest, out *pb.UpdatePlanResponse) error {
	log.Log("Update plan is called in dataflow service.")
	pl := _type.Plan{}
	pl.Id = bson.ObjectIdHex(in.Plan.Id)
	pl.Name = in.Plan.Name
	pl.Description = in.Plan.Description
	pl.Type = in.Plan.Type
	pl.SourceConnId = in.Plan.SourceConnId
	pl.DestConnId = in.Plan.DestConnId
	pl.SourceDir = in.Plan.SourceDir
	pl.DestDir = in.Plan.DestDir
	pl.OverWrite = in.Plan.OverWrite
	pl.RemainSource = in.Plan.RemainSource
	pl.PolicyId = in.Plan.PolicyId
	pl.Tenant = in.Plan.Tenant

	//TO-DO: Check validation of input parameter

	errcode := plan.Update(&pl)
	log.Logf("Update plan finished, err:%d", errcode)
	out.ErrCode = int64(errcode)

	return nil
}

func (b *dataflowService) GetJob(ctx context.Context, in *pb.GetJobRequest, out *pb.GetJobResponse) error {
	log.Log("Get job is called in dataflow service.")

	id := in.Id
	if in.Id == "All" {
		id = ""
	}

	//TO-DO: how to get tenant
	//tenant := in.Tenant
	tenant := "tenant"
	js,err := job.Get(id, tenant)
	out.ErrCode = int64(err)
	log.Logf("Get job err:%d.", out.ErrCode)
	//fmt.Printf("Getpolicy err:%d\n", rsp.ErrCode)
	if err == _type.ERR_OK {
		for i := 0; i < len(js); i++ {
			//des := "Total Capacity:" + js[i].TotalCapacity + ", "
			//TO-DO: need change according to real scenario
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
	return nil
}