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

package cli

import (
	"encoding/json"
	"os"
	"time"

	dataflow "github.com/opensds/multi-cloud/dataflow/proto"
	"github.com/spf13/cobra"
)

//----------------plan----------------
var planCommand = &cobra.Command{
	Use:   "plan",
	Short: "manage plans",
	Run:   backendAction,
}

var planCreateCommand = &cobra.Command{
	Use:   "create <plan info>",
	Short: "create a plan",
	Run:   planCreateAction,
}

var planListCommand = &cobra.Command{
	Use:   "list <plan info>",
	Short: "list all plans",
	Run:   planListAction,
}

var planShowCommand = &cobra.Command{
	Use:   "show <id>",
	Short: "show a plan",
	Run:   planShowAction,
}

var planUpdateCommand = &cobra.Command{
	Use:   "update <id>",
	Short: "update a plan",
	Run:   planUpdateAction,
}

var planDeleteCommand = &cobra.Command{
	Use:   "delete <id>",
	Short: "delete a plan",
	Run:   planDeleteAction,
}

var planRunCommand = &cobra.Command{
	Use:   "run  <id>",
	Short: "run a plan",
	Run:   planRunAction,
}

//----------------policy----------------
var policyCommand = &cobra.Command{
	Use:   "policy",
	Short: "manage policies",
	Run:   policyAction,
}

var policyCreateCommand = &cobra.Command{
	Use:   "create <policy info>",
	Short: "create a policy",
	Run:   policyCreateAction,
}

var policyShowCommand = &cobra.Command{
	Use:   "show <id>",
	Short: "show a policy",
	Run:   policyShowAction,
}

var policyListCommand = &cobra.Command{
	Use:   "list",
	Short: "list all policies",
	Run:   policyListAction,
}

var policyUpdateCommand = &cobra.Command{
	Use:   "update <id>",
	Short: "update a policy",
	Run:   policyUpdateAction,
}

var policyDeleteCommand = &cobra.Command{
	Use:   "delete  <id>",
	Short: "delete a policy",
	Run:   policyDeleteAction,
}

var (
	policyUpdateBody string
	planUpdateBody   string
)

//----------------job----------------
var jobCommand = &cobra.Command{
	Use:   "job",
	Short: "manage jobs",
	Run:   jobAction,
}

var jobListCommand = &cobra.Command{
	Use:   "list",
	Short: "list all jobs",
	Run:   jobListAction,
}

var jobShowCommand = &cobra.Command{
	Use:   "show",
	Short: "show a job",
	Run:   jobShowAction,
}

func init() {
	planCommand.AddCommand(planCreateCommand)
	planCommand.AddCommand(planListCommand)
	planCommand.AddCommand(planShowCommand)
	planCommand.AddCommand(planUpdateCommand)
	planUpdateCommand.Flags().StringVarP(&planUpdateBody, "body", "b", "", "the body of updated plan")
	planCommand.AddCommand(planDeleteCommand)
	planCommand.AddCommand(planRunCommand)

	policyCommand.AddCommand(policyCreateCommand)
	policyCommand.AddCommand(policyShowCommand)
	policyCommand.AddCommand(policyListCommand)
	policyCommand.AddCommand(policyUpdateCommand)
	policyUpdateCommand.Flags().StringVarP(&policyUpdateBody, "body", "b", "", "the body of updated policy")
	policyCommand.AddCommand(policyDeleteCommand)

	jobCommand.AddCommand(jobListCommand)
	jobCommand.AddCommand(jobShowCommand)
}

func planAction(cmd *cobra.Command, args []string) {
	cmd.Usage()
	os.Exit(1)
}

func policyAction(cmd *cobra.Command, args []string) {
	cmd.Usage()
	os.Exit(1)
}

func jobAction(cmd *cobra.Command, args []string) {
	cmd.Usage()
	os.Exit(1)
}

func planCreateAction(cmd *cobra.Command, args []string) {
	ArgsNumCheck(cmd, args, 1)
	plan := &dataflow.Plan{}
	if err := json.Unmarshal([]byte(args[0]), plan); err != nil {
		Errorln(err)
		cmd.Usage()
		os.Exit(1)
	}

	resp, err := client.CreatePlan(plan)
	if err != nil {
		Fatalln(HTTPErrStrip(err))
	}
	keys := KeyList{"Id", "Name", "Description", "Type", "PolicyId", "PolicyName",
		"SourceConn", "DestConn", "Filter", "RemainSource", "TenantId", "UserId",
		"PolicyEnabled"}
	PrintDict(resp, keys, FormatterList{})
}

func planListAction(cmd *cobra.Command, args []string) {
	ArgsNumCheck(cmd, args, 0)

	resp, err := client.ListPlan()
	if err != nil {
		Fatalln(HTTPErrStrip(err))
	}
	keys := KeyList{"Id", "Name", "Description", "Type", "PolicyId", "PolicyName",
		"SourceConn", "DestConn", "Filter", "RemainSource", "TenantId", "UserId",
		"PolicyEnabled"}
	PrintList(resp, keys, FormatterList{})
}

func planShowAction(cmd *cobra.Command, args []string) {
	ArgsNumCheck(cmd, args, 1)

	resp, err := client.ShowPlan(args[0])
	if err != nil {
		Fatalln(HTTPErrStrip(err))
	}
	keys := KeyList{"Id", "Name", "Description", "Type", "PolicyId", "PolicyName",
		"SourceConn", "DestConn", "Filter", "RemainSource", "TenantId", "UserId",
		"PolicyEnabled"}
	PrintDict(resp, keys, FormatterList{})
}

func planUpdateAction(cmd *cobra.Command, args []string) {
	ArgsNumCheck(cmd, args, 1)

	resp, err := client.UpdatePlan(args[0], planUpdateBody)
	if err != nil {
		Fatalln(HTTPErrStrip(err))
	}
	keys := KeyList{"Id", "Name", "Description", "Type", "PolicyId", "PolicyName",
		"SourceConn", "DestConn", "Filter", "RemainSource", "TenantId", "UserId",
		"PolicyEnabled"}
	PrintDict(resp, keys, FormatterList{})
}

func planDeleteAction(cmd *cobra.Command, args []string) {
	ArgsNumCheck(cmd, args, 1)

	err := client.DeletePlan(args[0])
	if err != nil {
		Fatalln(HTTPErrStrip(err))
	}
}

func planRunAction(cmd *cobra.Command, args []string) {
	ArgsNumCheck(cmd, args, 1)

	resp, err := client.RunPlan(args[0])
	if err != nil {
		Fatalln(HTTPErrStrip(err))
	}

	keys := KeyList{"JobId"}
	PrintDict(resp, keys, FormatterList{})
}

//-------------------------------------------------------------------
func policyCreateAction(cmd *cobra.Command, args []string) {
	ArgsNumCheck(cmd, args, 1)
	policy := &dataflow.Policy{}
	if err := json.Unmarshal([]byte(args[0]), policy); err != nil {
		Errorln(err)
		cmd.Usage()
		os.Exit(1)
	}

	resp, err := client.CreatePolicy(policy)
	if err != nil {
		Fatalln(HTTPErrStrip(err))
	}
	keys := KeyList{"Id", "Name", "Tenant", "Description", "Schedule"}
	PrintDict(resp, keys, FormatterList{})
}

func policyShowAction(cmd *cobra.Command, args []string) {
	ArgsNumCheck(cmd, args, 1)

	resp, err := client.ShowPolicy(args[0])
	if err != nil {
		Fatalln(HTTPErrStrip(err))
	}
	keys := KeyList{"Id", "Name", "Tenant", "Description", "Schedule"}
	PrintDict(resp, keys, FormatterList{})
}

func policyListAction(cmd *cobra.Command, args []string) {
	ArgsNumCheck(cmd, args, 0)

	resp, err := client.ListPolicy()
	if err != nil {
		Fatalln(HTTPErrStrip(err))
	}
	keys := KeyList{"Id", "Name", "Tenant", "Description", "Schedule"}
	PrintList(resp, keys, FormatterList{})
}

func policyUpdateAction(cmd *cobra.Command, args []string) {
	ArgsNumCheck(cmd, args, 1)

	resp, err := client.UpdatePolicy(args[0], policyUpdateBody)
	if err != nil {
		Fatalln(HTTPErrStrip(err))
	}
	keys := KeyList{"Id", "Name", "Tenant", "Description", "Schedule"}
	PrintDict(resp, keys, FormatterList{})
}

func policyDeleteAction(cmd *cobra.Command, args []string) {
	ArgsNumCheck(cmd, args, 1)

	err := client.DeletePolicy(args[0])
	if err != nil {
		Fatalln(HTTPErrStrip(err))
	}
}

// JobCliFormat implementation
type JobCliFormat struct {
	ID             string
	Type           string
	PlanName       string
	PlanID         string
	Description    string
	SourceLocation string
	DestLocation   string
	Status         string
	CreateTime     string
	StartTime      string
	EndTime        string
	RemainSource   bool
	TotalCapacity  int64
	PassedCapacity int64
	TotalCount     int64
	PassedCount    int64
	Progress       int64
}

// ConvertJobToCliFormat implementation
func ConvertJobToCliFormat(job *dataflow.Job) JobCliFormat {
	return JobCliFormat{
		ID:             job.Id,
		Type:           job.Type,
		PlanName:       job.PlanName,
		PlanID:         job.PlanId,
		Description:    job.Description,
		SourceLocation: job.SourceLocation,
		DestLocation:   job.DestLocation,
		Status:         job.Status,
		CreateTime:     time.Unix(job.CreateTime, 0).Format(`2006-01-02T15:04:05`),
		StartTime:      time.Unix(job.StartTime, 0).Format(`2006-01-02T15:04:05`),
		EndTime:        time.Unix(job.EndTime, 0).Format(`2006-01-02T15:04:05`),
		RemainSource:   job.RemainSource,
		TotalCapacity:  job.TotalCapacity,
		PassedCapacity: job.PassedCapacity,
		TotalCount:     job.TotalCount,
		PassedCount:    job.PassedCount,
		Progress:       job.Progress,
	}
}

func jobListAction(cmd *cobra.Command, args []string) {
	ArgsNumCheck(cmd, args, 0)

	resp, err := client.ListJob()
	if err != nil {
		Fatalln(HTTPErrStrip(err))
	}

	var jobs []JobCliFormat
	for _, oldJob := range resp {
		jobs = append(jobs, ConvertJobToCliFormat(oldJob))
	}

	keys := KeyList{"ID", "Type", "PlanName", "PlanID", "SourceLocation",
		"DestLocation", "Status", "CreateTime", "StartTime", "EndTime", "RemainSource",
		"TotalCapacity", "PassedCapacity", "TotalCount", "PassedCount", "Progress"}
	PrintList(jobs, keys, FormatterList{})
}

func jobShowAction(cmd *cobra.Command, args []string) {
	ArgsNumCheck(cmd, args, 1)

	resp, err := client.ShowJob(args[0])
	if err != nil {
		Fatalln(HTTPErrStrip(err))
	}

	keys := KeyList{"ID", "Type", "PlanName", "PlanID", "SourceLocation",
		"DestLocation", "Status", "CreateTime", "StartTime", "EndTime", "RemainSource",
		"TotalCapacity", "PassedCapacity", "TotalCount", "PassedCount", "Progress"}
	PrintDict(ConvertJobToCliFormat(resp), keys, FormatterList{})
}
