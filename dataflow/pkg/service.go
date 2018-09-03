package pkg

import (
	"context"

	"github.com/micro/go-log"
	pb "github.com/opensds/go-panda/dataflow/proto"
	"github.com/opensds/go-panda/dataflow/pkg/policy"
	"github.com/opensds/go-panda/dataflow/pkg/type"
	"github.com/opensds/go-panda/dataflow/pkg/db"
	"os"
	. "github.com/opensds/go-panda/dataflow/pkg/utils"
	"encoding/json"
)

type dataflowService struct{}

func (b *dataflowService) GetPolicy(ctx context.Context, in *pb.GetPolicyRequest, out *pb.GetPolicyResponse) error {
	log.Log("Getdataflow is called in dataflow service.")
	//out.Id = "c506cd4b-9048-43bc-97ef-0d7dec369b42"
	//out.Name = "GetPolicy." + in.Id

	name := in.Name
	//TO-DO: how to get tenant
	//tenant := in.Tenant
	tenant := "tenant"
	pols,err := policy.Get(name, tenant)
	out.ErrCode = int32(err)
	log.Log("Getpolicy err:%d.", out.ErrCode)
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
	jsons1, errs1 := json.Marshal(out.Pols)
	if errs1 != nil {
		log.Logf(errs1.Error())
	}else {
		log.Logf("jsons1: %s.\n", jsons1)
	}
	//For debug -- end
	return nil
}

func (b *dataflowService) CreatePolicy(ctx context.Context, in *pb.CreatePolicyRequest, out *pb.CreatePolicyResponse) error {
	pol := _type.Policy{}
	pol.Name = in.Pol.Name
	pol.Tenant = in.Pol.Tenant
	pol.Description = in.Pol.Description
	pol.Schedule.Day = in.Pol.Sched.Days
	pol.Schedule.TimePoint = in.Pol.Sched.TimePoint
	pol.Schedule.Type = in.Pol.Sched.Type

	rsp := pb.CreatePolicyResponse{}
	errcode := policy.Create(&pol)
	rsp.ErrCode = int32(errcode)

	return nil
}

func (b *dataflowService) DeletePolicy(ctx context.Context, in *pb.GetPolicyRequest, out *pb.GetPolicyResponse) error {

	return nil
}

func (b *dataflowService) UpdatePolicy(ctx context.Context, in *pb.GetPolicyRequest, out *pb.GetPolicyResponse) error {

	return nil
}

func NewDataFlowService() pb.DataFlowHandler {
	host := os.Getenv("DB_HOST")
	dbstor := Database{Credential:"unkonwn", Driver:"mongodb", Endpoint:host}
	db.Init(&dbstor)
	return &dataflowService{}
}
